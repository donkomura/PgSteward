use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use pgsteward_sched::{Allocator, Claim};
use tokio::sync::watch;

use crate::allocation::{
    AllocationTable, Desired, Entry, Holder, InstanceId, PreconditionError, ProxyId,
};
use crate::rt::Clock;
use crate::tenant::TenantId;

type Slot = (InstanceId, TenantId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantPolicy {
    pub min: u32,
    pub max: u32,
    pub weight: NonZeroU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Usage {
    pub demand: u32,
    pub actual: u32,
}

/// Everything a proxy reports in one round, as of the grants of `generation`.
///
/// A report is complete: an instance and tenant it leaves out has no demand
/// and no connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    generation: u64,
    usage: BTreeMap<Slot, Usage>,
}

impl Report {
    #[must_use]
    pub fn new(generation: u64) -> Self {
        Self {
            generation,
            usage: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn usage(mut self, instance: InstanceId, tenant: TenantId, usage: Usage) -> Self {
        self.usage.insert((instance, tenant), usage);
        self
    }

    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn get(&self, instance: &InstanceId, tenant: &TenantId) -> Usage {
        self.usage
            .get(&(instance.clone(), tenant.clone()))
            .copied()
            .unwrap_or_default()
    }
}

/// The grants one proxy holds. The generation moves whenever a grant does, so a
/// report can say which grants it was measured against.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GrantSet {
    generation: u64,
    grants: BTreeMap<Slot, u32>,
}

impl GrantSet {
    #[must_use]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    #[must_use]
    pub fn get(&self, instance: &InstanceId, tenant: &TenantId) -> u32 {
        self.grants
            .get(&(instance.clone(), tenant.clone()))
            .copied()
            .unwrap_or(0)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&InstanceId, &TenantId, u32)> {
        self.grants
            .iter()
            .map(|((instance, tenant), slots)| (instance, tenant, *slots))
    }
}

/// The only way a proxy learns what it may hold and tells what it holds.
///
/// The proxy never decides a grant. It receives them here and reports its
/// demand and its actual connections back; where the grants are computed is
/// hidden behind this trait.
pub trait GrantChannel: Send + Sync {
    fn grants(&self) -> watch::Receiver<GrantSet>;
    fn report(&self, report: Report);
}

/// The degenerate form of the coordinator: the allocation table, the allocator
/// and the control loop in the proxy's own process, granting to that proxy.
#[derive(Debug)]
pub struct InProcessCoordinator<A> {
    proxy: ProxyId,
    allocator: A,
    state: Mutex<State>,
    grants: watch::Sender<GrantSet>,
}

#[derive(Debug, Default)]
struct State {
    table: AllocationTable,
    budgets: BTreeMap<InstanceId, u32>,
    policies: BTreeMap<Slot, TenantPolicy>,
    demand: BTreeMap<Slot, u32>,
    held: BTreeMap<Slot, Held>,
    generation: u64,
}

/// How many connections a holder may still have on the instance.
///
/// A grant that went up counts in full at once, since the proxy may open up to
/// it at any moment. A grant that went down keeps counting what it was until a
/// report measured against the lowered grant says the connections are gone.
#[derive(Debug, Clone, Copy, Default)]
struct Held {
    changed_at: u64,
    occupied: u32,
}

impl<A: Allocator<Holder>> InProcessCoordinator<A> {
    #[must_use]
    pub fn new(proxy: ProxyId, allocator: A) -> Self {
        let (grants, _) = watch::channel(GrantSet::default());
        Self {
            proxy,
            allocator,
            state: Mutex::new(State::default()),
            grants,
        }
    }

    pub fn set_budget(&self, instance: InstanceId, budget: u32) {
        self.lock().budgets.insert(instance, budget);
    }

    pub fn set_policy(&self, instance: InstanceId, tenant: TenantId, policy: TenantPolicy) {
        self.lock().policies.insert((instance, tenant), policy);
    }

    #[must_use]
    pub fn table(&self) -> AllocationTable {
        self.lock().table.clone()
    }

    pub fn reconcile(&self) -> Result<(), PreconditionError> {
        let mut state = self.lock();
        let entry = self.desired_state(&state);
        let previous = self.published(&state.table);
        state.table.apply(&entry)?;
        let next = self.published(&state.table);
        if next == previous {
            return Ok(());
        }
        state.generation += 1;
        let generation = state.generation;
        for slot in previous.keys().chain(next.keys()) {
            let before = previous.get(slot).copied().unwrap_or(0);
            let after = next.get(slot).copied().unwrap_or(0);
            if before != after {
                let held = state.held.entry(slot.clone()).or_default();
                held.changed_at = generation;
                held.occupied = held.occupied.max(after);
            }
        }
        state
            .held
            .retain(|slot, held| held.occupied > 0 || next.contains_key(slot));
        self.grants.send_replace(GrantSet {
            generation,
            grants: next,
        });
        Ok(())
    }

    pub async fn run<K: Clock>(&self, clock: &K, interval: Duration) {
        loop {
            if let Err(error) = self.reconcile() {
                tracing::error!(%error, "the desired state was rejected by the allocation table");
            }
            clock.sleep(interval).await;
        }
    }

    fn desired_state(&self, state: &State) -> Entry {
        let mut entry = Entry::new();
        for (instance, &budget) in &state.budgets {
            let claims = self.claims(state, instance);
            let targets = self.allocator.allocate(budget, &claims);
            let tenants: BTreeSet<&TenantId> = state
                .policies
                .keys()
                .chain(state.held.keys())
                .filter(|(of, _)| of == instance)
                .map(|(_, tenant)| tenant)
                .collect();
            let occupied: u32 = tenants
                .iter()
                .map(|tenant| state.occupied(instance, tenant))
                .sum();
            let mut available = budget.saturating_sub(occupied);
            let mut desired = Desired::new(budget);
            for tenant in tenants {
                let holder = self.holder(tenant);
                let current = state.table.granted(instance, &holder);
                let target = targets.get(&holder);
                let granted = if target <= current {
                    target
                } else {
                    let occupied = state.occupied(instance, tenant);
                    let granted = target.min(occupied + available);
                    available -= granted.saturating_sub(occupied);
                    granted
                };
                desired = desired.grant(holder, granted);
            }
            entry = entry.instance(instance.clone(), desired);
        }
        entry
    }

    fn claims(&self, state: &State, instance: &InstanceId) -> Vec<Claim<Holder>> {
        state
            .policies
            .iter()
            .filter(|((of, _), _)| of == instance)
            .map(|(slot, policy)| {
                let holder = self.holder(&slot.1);
                Claim {
                    min: policy.min,
                    max: policy.max,
                    weight: policy.weight,
                    demand: state.demand.get(slot).copied().unwrap_or(0),
                    current: state.table.granted(instance, &holder),
                    key: holder,
                }
            })
            .collect()
    }

    fn published(&self, table: &AllocationTable) -> BTreeMap<Slot, u32> {
        table
            .grants_for(&self.proxy)
            .map(|(instance, tenant, slots)| ((instance.clone(), tenant.clone()), slots))
            .collect()
    }

    fn holder(&self, tenant: &TenantId) -> Holder {
        Holder::new(tenant.clone(), self.proxy.clone())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("coordinator state lock poisoned")
    }
}

impl State {
    fn occupied(&self, instance: &InstanceId, tenant: &TenantId) -> u32 {
        self.held
            .get(&(instance.clone(), tenant.clone()))
            .map_or(0, |held| held.occupied)
    }
}

impl<A: Allocator<Holder> + Send + Sync> GrantChannel for InProcessCoordinator<A> {
    fn grants(&self) -> watch::Receiver<GrantSet> {
        self.grants.subscribe()
    }

    fn report(&self, report: Report) {
        let mut state = self.lock();
        state.demand = report
            .usage
            .iter()
            .map(|(slot, usage)| (slot.clone(), usage.demand))
            .collect();
        let granted = self.published(&state.table);
        for (slot, held) in &mut state.held {
            if report.generation >= held.changed_at {
                let actual = report.get(&slot.0, &slot.1).actual;
                let grant = granted.get(slot).copied().unwrap_or(0);
                held.occupied = grant.max(actual);
            }
        }
        state
            .held
            .retain(|slot, held| held.occupied > 0 || granted.contains_key(slot));
    }
}
