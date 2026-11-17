use std::collections::{hash_map, HashMap};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

/// `TransactionTracker` tracks the state of transactions to detect retransmissions.
pub struct TransactionTracker {
    retention_period: Duration,
    transactions: Mutex<Transactions>,
}

/// skyfs patch: expired entries are swept at most once per
/// [`HOUSEKEEPING_INTERVAL`]. Upstream swept the whole map on every request
/// while holding the lock: with a 60 s retention a busy client keeps about
/// 180,000 entries, so every RPC scanned them all, serialized.
struct Transactions {
    map: HashMap<(u32, String), TransactionState>,
    last_housekeeping: SystemTime,
}

/// How often expired transactions are swept.
const HOUSEKEEPING_INTERVAL: Duration = Duration::from_secs(1);

impl TransactionTracker {
    pub fn new(retention_period: Duration) -> Self {
        Self {
            retention_period,
            transactions: Mutex::new(Transactions {
                map: HashMap::new(),
                last_housekeeping: SystemTime::now(),
            }),
        }
    }

    /// Checks if the transaction is a retransmission.
    /// If it's a new transaction, it is marked as `InProgress`.
    ///
    /// Returns `true` if the transaction is a retransmission, `false` otherwise.
    pub fn is_retransmission(&self, xid: u32, client_addr: &str) -> bool {
        let key = (xid, client_addr.to_string());
        let mut transactions = self
            .transactions
            .lock()
            .expect("unable to unlock transactions mutex");
        let now = SystemTime::now();
        if now
            .duration_since(transactions.last_housekeeping)
            .map_or(true, |d| d >= HOUSEKEEPING_INTERVAL)
        {
            housekeeping(&mut transactions.map, self.retention_period);
            transactions.last_housekeeping = now;
        }
        if let hash_map::Entry::Vacant(e) = transactions.map.entry(key) {
            e.insert(TransactionState::InProgress);
            false
        } else {
            true
        }
    }

    /// Marks the transaction as processed.
    pub fn mark_processed(&self, xid: u32, client_addr: &str) {
        let key = (xid, client_addr.to_string());
        let completion_time = SystemTime::now();
        let mut transactions = self
            .transactions
            .lock()
            .expect("unable to unlock transactions mutex");
        if let Some(tx) = transactions.map.get_mut(&key) {
            *tx = TransactionState::Completed(completion_time);
        }
    }
}

fn housekeeping(transactions: &mut HashMap<(u32, String), TransactionState>, max_age: Duration) {
    let mut cutoff = SystemTime::now() - max_age;
    transactions.retain(|_, v| match v {
        TransactionState::InProgress => true,
        TransactionState::Completed(completion_time) => completion_time >= &mut cutoff,
    });
}

pub enum TransactionState {
    InProgress,
    Completed(SystemTime),
}
