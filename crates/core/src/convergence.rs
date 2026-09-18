use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

use crate::allocation::InstanceId;
use crate::grant::{GrantChannel, GrantSet, Report, Usage};
use crate::pool::{CloseServer, OpenServer, Pool};
use crate::relay::Welcome;
use crate::rt::Clock;
use crate::tenant::TenantId;

type Key = (InstanceId, TenantId);

/// The proxy's pools, one per instance and tenant, each sized by the grant it
/// is given and nothing else.
///
/// A pool exists only while its tenant is in use here: it is opened by the
/// first client that needs it and dropped by the convergence that finds it
/// without a grant, without a connection and without anyone holding it.
pub struct ProxyPools<O: OpenServer, K: Clock> {
    pools: Mutex<BTreeMap<Key, Entry<O, K>>>,
}

struct Entry<O: OpenServer, K: Clock> {
    pool: Pool<O, K>,
    welcome: Welcome,
}

impl<O: OpenServer, K: Clock> ProxyPools<O, K> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn insert(&self, instance: InstanceId, tenant: TenantId, pool: Pool<O, K>) {
        self.lock().insert(
            (instance, tenant),
            Entry {
                pool,
                welcome: Welcome::default(),
            },
        );
    }

    #[must_use]
    pub fn get(&self, instance: &InstanceId, tenant: &TenantId) -> Option<Pool<O, K>> {
        self.lock()
            .get(&(instance.clone(), tenant.clone()))
            .map(|entry| entry.pool.clone())
    }

    pub fn checkout(
        &self,
        instance: &InstanceId,
        tenant: &TenantId,
        open: impl FnOnce() -> Pool<O, K>,
    ) -> (Pool<O, K>, Welcome) {
        let mut pools = self.lock();
        let entry = pools
            .entry((instance.clone(), tenant.clone()))
            .or_insert_with(|| Entry {
                pool: open(),
                welcome: Welcome::default(),
            });
        (entry.pool.clone(), entry.welcome.clone())
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    /// Measures every pool against the grants of `generation`.
    ///
    /// Connections still opening count as actual: the instance may already hold
    /// them, and a slot reported free while one is on its way would be granted
    /// to someone else.
    #[must_use]
    pub fn report(&self, generation: u64) -> Report {
        self.lock().iter().fold(
            Report::new(generation),
            |report, ((instance, tenant), entry)| {
                let stats = entry.pool.stats();
                report.usage(
                    instance.clone(),
                    tenant.clone(),
                    Usage {
                        demand: saturate(stats.demand()),
                        actual: saturate(stats.occupied()),
                    },
                )
            },
        )
    }

    fn snapshot(&self) -> Vec<(Key, Pool<O, K>)> {
        self.lock()
            .iter()
            .map(|(key, entry)| (key.clone(), entry.pool.clone()))
            .collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<Key, Entry<O, K>>> {
        self.pools.lock().expect("proxy pools lock poisoned")
    }
}

impl<O: OpenServer, K: Clock> ProxyPools<O, K>
where
    O::Connection: CloseServer,
{
    pub async fn converge(&self, grants: &GrantSet) -> usize {
        let mut closed = 0;
        for ((instance, tenant), pool) in self.snapshot() {
            let grant = grants.get(&instance, &tenant) as usize;
            closed += pool.converge(grant).await;
        }
        self.lock().retain(|(instance, tenant), entry| {
            grants.get(instance, tenant) > 0 || !entry.pool.is_unused()
        });
        closed
    }

    /// Converges to the latest grants and reports what that left, whenever the
    /// grants change and at least once per `interval`. The report always
    /// follows the convergence, so it is measured against the grants it names.
    pub async fn run<G: GrantChannel + ?Sized>(&self, channel: &G, clock: &K, interval: Duration) {
        let mut grants = channel.grants();
        loop {
            let current = grants.borrow_and_update().clone();
            self.converge(&current).await;
            channel.report(self.report(current.generation()));
            tokio::select! {
                changed = grants.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
                () = clock.sleep(interval) => {}
            }
        }
    }
}

impl<O: OpenServer, K: Clock> Default for ProxyPools<O, K> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: OpenServer, K: Clock> fmt::Debug for ProxyPools<O, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.lock().iter().map(|(key, entry)| (key, &entry.pool)))
            .finish()
    }
}

fn saturate(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}
