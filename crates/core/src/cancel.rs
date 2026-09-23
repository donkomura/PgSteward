use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::BytesMut;
use pgsteward_protocol::startup::{CancelKey, StartupRequest, encode_startup};
use rand::RngExt;
use tokio::io::AsyncWriteExt;

use crate::allocation::{InstanceId, ProxyId};
use crate::rt::Net;

const SERIAL_BITS: u32 = 19;
const SERIAL_MASK: u32 = (1 << SERIAL_BITS) - 1;

/// Which proxy issued a cancel key, carried in the high bits of the process id
/// it hands to the client.
///
/// A `CancelRequest` reaches whichever node the client happens to connect to,
/// which in M2 need not be the node holding the server connection. The tag is
/// what lets that node forward the request instead of dropping it. Until then
/// it is derived from the proxy identifier; the cluster assigns it once there
/// is a membership to assign it from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProxyTag(u16);

impl ProxyTag {
    pub const MAX: u16 = (1 << (31 - SERIAL_BITS)) - 1;

    #[must_use]
    pub fn new(value: u16) -> Option<Self> {
        (value <= Self::MAX).then_some(Self(value))
    }

    #[must_use]
    pub fn of(proxy: &ProxyId) -> Self {
        let mut hash: u32 = 0x811c_9dc5;
        for byte in proxy.as_str().as_bytes() {
            hash ^= u32::from(*byte);
            hash = hash.wrapping_mul(0x0100_0193);
        }
        Self(twelve_bits((hash >> 16) ^ hash))
    }

    #[must_use]
    pub fn in_key(key: CancelKey) -> Self {
        Self(twelve_bits(
            u32::from_ne_bytes(key.process_id.to_ne_bytes()) >> SERIAL_BITS,
        ))
    }

    #[must_use]
    pub fn get(self) -> u16 {
        self.0
    }
}

fn twelve_bits(bits: u32) -> u16 {
    u16::try_from(bits & u32::from(ProxyTag::MAX)).expect("the mask leaves twelve bits")
}

/// The backend a `CancelRequest` is meant for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    instance: InstanceId,
    backend: CancelKey,
}

impl Target {
    #[must_use]
    pub fn instance(&self) -> &InstanceId {
        &self.instance
    }

    #[must_use]
    pub fn backend(&self) -> CancelKey {
        self.backend
    }
}

type Live = Arc<Mutex<HashMap<CancelKey, Option<Target>>>>;

/// The keys this node has handed out and what each of them currently points at.
///
/// The key a client gets is this system's own, unrelated to the key of the
/// server connection it happens to be served by: a client may be served by
/// several backends over its life, and a backend by several clients, so passing
/// the server's key on would let a client cancel a query that is no longer its
/// own.
#[derive(Debug, Clone)]
pub struct CancelRegistry {
    tag: ProxyTag,
    serial: Arc<AtomicU32>,
    live: Live,
}

impl CancelRegistry {
    #[must_use]
    pub fn new(tag: ProxyTag) -> Self {
        Self {
            tag,
            serial: Arc::new(AtomicU32::new(0)),
            live: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    #[must_use]
    pub fn issue(&self) -> Ticket {
        let serial = self.serial.fetch_add(1, Ordering::Relaxed) & SERIAL_MASK;
        let bits = (u32::from(self.tag.get()) << SERIAL_BITS) | serial;
        let key = CancelKey {
            process_id: i32::try_from(bits).expect("the sign bit stays clear"),
            secret_key: rand::rng().random(),
        };
        self.lock().insert(key, None);
        Ticket {
            key,
            live: Arc::clone(&self.live),
        }
    }

    /// Where a `CancelRequest` that carries `key` must go, if anywhere.
    ///
    /// A key that was never issued, one whose secret does not match, and one
    /// whose client is between requests all resolve to nothing. The last is
    /// what keeps a `CancelRequest` that arrives after its query finished from
    /// stopping whatever the connection runs next.
    #[must_use]
    pub fn target(&self, key: CancelKey) -> Option<Target> {
        self.lock().get(&key).cloned().flatten()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<CancelKey, Option<Target>>> {
        self.live.lock().expect("cancel registry lock poisoned")
    }
}

/// One client's cancel key, live for as long as the client is.
#[derive(Debug)]
pub struct Ticket {
    key: CancelKey,
    live: Live,
}

impl Ticket {
    #[must_use]
    pub fn key(&self) -> CancelKey {
        self.key
    }

    pub fn aim(&self, instance: InstanceId, backend: CancelKey) {
        self.lock()
            .insert(self.key, Some(Target { instance, backend }));
    }

    pub fn stand_down(&self) {
        self.lock().insert(self.key, None);
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<CancelKey, Option<Target>>> {
        self.live.lock().expect("cancel registry lock poisoned")
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.lock().remove(&self.key);
    }
}

/// Asks an instance to cancel what `backend` is running.
///
/// A `CancelRequest` travels on a connection of its own, which the instance
/// answers by closing without a reply. The connection is outside the allocation
/// table on purpose: it carries no query, lives for one round trip, and the
/// client that asked for the cancel is the one holding the slot being stopped.
pub async fn forward_cancel<N: Net>(net: &N, address: &str, backend: CancelKey) -> io::Result<()> {
    let mut stream = net.connect(address).await?;
    let mut out = BytesMut::new();
    encode_startup(&StartupRequest::Cancel(backend), &mut out);
    stream.write_all(&out).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}
