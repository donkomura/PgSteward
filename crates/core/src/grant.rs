use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use pgsteward_sched::{Allocator, Claim};
use tokio::sync::watch;

use crate::allocation::{
    AllocationTable, Desired, Entry, Holder, InstanceId, PreconditionError, ProxyId,
};
use crate::budget::{BudgetChange, InstanceBudget, TotalBudget};
use crate::policy::{Policies, PolicyChange, SettingError};
use crate::rt::{Clock, Instant};
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
    release_delay: Duration,
    state: Mutex<State>,
    grants: watch::Sender<GrantSet>,
    /// Ticks once per report. A setting that waits for the proxies to
    /// converge is woken by it, since a report is the only thing that tells
    /// the coordinator a connection is gone.
    reports: watch::Sender<u64>,
}

#[derive(Debug, Default)]
struct State {
    table: AllocationTable,
    budgets: BTreeMap<InstanceId, InstanceBudget>,
    policies: Policies,
    demand: BTreeMap<Slot, u32>,
    held: BTreeMap<Slot, Held>,
    generation: u64,
    departed: bool,
    now: Option<Instant>,
    idle_since: BTreeMap<Slot, Instant>,
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
        let (reports, _) = watch::channel(0);
        Self {
            proxy,
            allocator,
            release_delay: Duration::ZERO,
            state: Mutex::new(State::default()),
            grants,
            reports,
        }
    }

    #[must_use]
    pub fn with_release_delay(mut self, delay: Duration) -> Self {
        self.release_delay = delay;
        self
    }

    /// Takes `instance` under the coordinator with the total budget derived
    /// for it. The budget is derived here rather than reported as a number,
    /// because the margin it deducts is a cluster setting an operator writes
    /// and a setting has one home.
    pub fn add_instance(&self, instance: InstanceId, budget: InstanceBudget) {
        self.lock().budgets.insert(instance, budget);
    }

    /// Re-derives an instance's total budget from the connections that belong
    /// to someone else, and answers which way it moved. An instance the
    /// coordinator does not hold is not observed at all.
    pub fn observe_instance(
        &self,
        instance: &InstanceId,
        at: Instant,
        foreign_connections: u32,
    ) -> Option<BudgetChange> {
        self.lock()
            .budgets
            .get_mut(instance)
            .map(|budget| budget.observe(at, foreign_connections))
    }

    /// Each instance's total budget together with the parts it was derived
    /// from, in the order the instances are named.
    #[must_use]
    pub fn instances(&self) -> Vec<(InstanceId, TotalBudget)> {
        self.lock()
            .budgets
            .iter()
            .map(|(instance, budget)| (instance.clone(), budget.current()))
            .collect()
    }

    pub fn set_policies(&self, policies: Policies) {
        self.lock().policies = policies;
    }

    #[must_use]
    pub fn policies(&self) -> Policies {
        self.lock().policies.clone()
    }

    /// Writes one tenant rule of the cluster configuration and recomputes the
    /// desired state from it, so that a setting an operator wrote is in force
    /// when the command is answered rather than at the next round.
    pub fn set_tenant(&self, tenant: &str, change: PolicyChange) -> Result<(), SettingError> {
        {
            let mut state = self.lock();
            let changed = state.policies.change_tenant(tenant, change, |instance| {
                state
                    .budgets
                    .get(instance)
                    .map(|budget| budget.current().total())
            })?;
            state.policies = changed;
        }
        if let Err(error) = self.reconcile() {
            tracing::error!(%error, "the desired state was rejected by the allocation table");
        }
        Ok(())
    }

    /// Writes the margin an instance's total budget is derived with, and
    /// answers once that budget is in force.
    ///
    /// A margin that grows the budget is in force as soon as the desired state
    /// has been computed from it. A margin that shrinks it is not: until the
    /// proxies have closed what they hold above the new budget, the instance
    /// carries more connections than the setting allows, so the answer waits
    /// for them to report the excess gone.
    pub async fn set_margin(&self, instance: &InstanceId, margin: u32) -> Result<(), SettingError> {
        let change = {
            let mut state = self.lock();
            let budget =
                state
                    .budgets
                    .get(instance)
                    .ok_or_else(|| SettingError::NoSuchInstance {
                        instance: instance.clone(),
                    })?;
            let derived = budget.with_margin(margin).total();
            let minimums = state.policies.minimums_on(instance);
            if minimums > derived {
                return Err(SettingError::AboveBudget {
                    instance: instance.clone(),
                    minimums,
                    budget: derived,
                });
            }
            state
                .budgets
                .get_mut(instance)
                .expect("the budget was found above")
                .set_margin(margin)
        };
        if let Err(error) = self.reconcile() {
            tracing::error!(%error, "the desired state was rejected by the allocation table");
        }
        if matches!(change, BudgetChange::Shrank { .. }) {
            self.converged(instance).await;
        }
        Ok(())
    }

    /// Returns once no more connections can be held on `instance` than its
    /// budget allows.
    async fn converged(&self, instance: &InstanceId) {
        let mut reports = self.reports.subscribe();
        while !self.within_budget(instance) {
            if reports.changed().await.is_err() {
                return;
            }
        }
    }

    fn within_budget(&self, instance: &InstanceId) -> bool {
        let state = self.lock();
        let budget = state
            .budgets
            .get(instance)
            .map_or(0, |budget| budget.current().total());
        state.occupied_on(instance) <= budget
    }

    /// Gives every grant this proxy holds back to its instances, and leaves it
    /// granted nothing from then on.
    ///
    /// A proxy that is stopping calls it once the connections behind those
    /// grants are closed. From this moment the slots are free for another
    /// holder to open into, so a proxy that still held its connections would
    /// put the instance over its total budget.
    pub fn withdraw(&self) {
        {
            let mut state = self.lock();
            state.departed = true;
            state.demand.clear();
            state.held.clear();
        }
        if let Err(error) = self.reconcile() {
            tracing::error!(%error, "the desired state was rejected by the allocation table");
        }
    }

    #[must_use]
    pub fn table(&self) -> AllocationTable {
        self.lock().table.clone()
    }

    pub fn reconcile_at(&self, now: Instant) -> Result<(), PreconditionError> {
        self.lock().now = Some(now);
        self.reconcile()
    }

    pub fn reconcile(&self) -> Result<(), PreconditionError> {
        let mut state = self.lock();
        self.track_idle(&mut state);
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
            if let Err(error) = self.reconcile_at(clock.now()) {
                tracing::error!(%error, "the desired state was rejected by the allocation table");
            }
            clock.sleep(interval).await;
        }
    }

    fn desired_state(&self, state: &State) -> Entry {
        let mut entry = Entry::new();
        for (instance, derived) in &state.budgets {
            let budget = derived.current().total();
            if state.departed {
                entry = entry.instance(instance.clone(), Desired::new(budget));
                continue;
            }
            let tenants: BTreeSet<&TenantId> = state
                .demand
                .keys()
                .chain(state.held.keys())
                .filter(|(of, _)| of == instance)
                .map(|(_, tenant)| tenant)
                .collect();
            let claims = self.claims(state, instance, &tenants);
            let targets = self.allocator.allocate(budget, &claims);
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

    fn claims(
        &self,
        state: &State,
        instance: &InstanceId,
        tenants: &BTreeSet<&TenantId>,
    ) -> Vec<Claim<Holder>> {
        tenants
            .iter()
            .filter_map(|tenant| {
                let policy = state.policies.policy(instance, tenant)?;
                let holder = self.holder(tenant);
                Some(Claim {
                    min: policy.min,
                    max: policy.max,
                    weight: policy.weight,
                    demand: state
                        .demand
                        .get(&(instance.clone(), (*tenant).clone()))
                        .copied()
                        .unwrap_or(0),
                    current: state.table.granted(instance, &holder),
                    may_release: self.may_release(state, instance, tenant),
                    key: holder,
                })
            })
            .collect()
    }

    fn track_idle(&self, state: &mut State) {
        let Some(now) = state.now else {
            return;
        };
        let idle: BTreeSet<Slot> = self
            .published(&state.table)
            .into_iter()
            .filter(|(slot, granted)| state.demand.get(slot).copied().unwrap_or(0) < *granted)
            .map(|(slot, _)| slot)
            .collect();
        state.idle_since.retain(|slot, _| idle.contains(slot));
        for slot in idle {
            state.idle_since.entry(slot).or_insert(now);
        }
    }

    fn may_release(&self, state: &State, instance: &InstanceId, tenant: &TenantId) -> bool {
        let slot = (instance.clone(), tenant.clone());
        match (state.now, state.idle_since.get(&slot)) {
            (Some(now), Some(since)) => now.duration_since(*since) >= self.release_delay,
            _ => true,
        }
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

    fn occupied_on(&self, instance: &InstanceId) -> u32 {
        self.held
            .iter()
            .filter(|((of, _), _)| of == instance)
            .map(|(_, held)| held.occupied)
            .fold(0, u32::saturating_add)
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
        drop(state);
        self.reports
            .send_modify(|count| *count = count.wrapping_add(1));
    }
}

#[derive(Debug)]
pub struct ProxyChannel<A> {
    coordinator: Arc<InProcessCoordinator<A>>,
    proxy: ProxyId,
}

impl<A: Allocator<Holder>> InProcessCoordinator<A> {
    #[must_use]
    pub fn channel(self: &Arc<Self>, proxy: ProxyId) -> ProxyChannel<A> {
        ProxyChannel {
            coordinator: Arc::clone(self),
            proxy,
        }
    }
}

impl<A: Allocator<Holder>> ProxyChannel<A> {
    #[must_use]
    pub fn proxy(&self) -> &ProxyId {
        &self.proxy
    }

    pub fn withdraw(&self) {}
}

impl<A: Allocator<Holder> + Send + Sync> GrantChannel for ProxyChannel<A> {
    fn grants(&self) -> watch::Receiver<GrantSet> {
        watch::channel(GrantSet::default()).1
    }

    fn report(&self, _report: Report) {}
}
