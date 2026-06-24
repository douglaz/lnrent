//! Injectable time source. SPEC.md §6.5, lnrent-7fp.2. Every deadline / timestamp the
//! daemon computes (invoice expiry, `paid_through` / `soft_date`, reconcile deadlines,
//! downtime credit) keys on this, so time-based transitions are deterministic under test
//! with no real sleeps.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// Wall-clock time as **unix seconds**. Injected wherever deadlines / timestamps are
/// computed (store, reconcile), so a `TestClock` can drive time in tests.
pub trait Clock: Send + Sync {
    fn now(&self) -> i64;
}

/// The real wall clock.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_secs() as i64
    }
}

/// A deterministic, settable/advanceable test clock. Cheap to clone — clones share the same
/// instant — so a test and the code under test observe the same time.
#[derive(Debug, Clone)]
pub struct TestClock(Arc<AtomicI64>);

impl TestClock {
    /// A clock pinned at `start` (unix seconds).
    pub fn new(start: i64) -> Self {
        TestClock(Arc::new(AtomicI64::new(start)))
    }
    /// Jump to an absolute time.
    pub fn set(&self, t: i64) {
        self.0.store(t, Ordering::SeqCst);
    }
    /// Advance by `secs` and return the new time.
    pub fn advance(&self, secs: i64) -> i64 {
        self.0.fetch_add(secs, Ordering::SeqCst) + secs
    }
}

impl Clock for TestClock {
    fn now(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_returns_unix_seconds() {
        // After 2024-01-01 (1_704_067_200); a sanity floor that the real clock is wall time.
        assert!(SystemClock.now() > 1_704_067_200);
    }

    #[test]
    fn test_clock_set_and_advance() {
        let c = TestClock::new(1_000);
        assert_eq!(c.now(), 1_000);
        assert_eq!(c.advance(30), 1_030);
        assert_eq!(c.now(), 1_030);
        c.set(5_000);
        assert_eq!(c.now(), 5_000);
        // A clone shares the same instant (injection into code-under-test).
        let c2 = c.clone();
        c.advance(7);
        assert_eq!(c2.now(), 5_007);
    }

    // A deadline "fires" the moment the clock reaches it — driven by advancing the clock,
    // with no real sleep (the property reconcile relies on, §6.5).
    #[test]
    fn deadline_fires_under_test_clock_without_sleeping() {
        let tc = TestClock::new(100);
        let clock: &dyn Clock = &tc; // injected as a trait object, as reconcile will take it
        let deadline = 130;
        let due = |c: &dyn Clock| c.now() >= deadline;

        assert!(!due(clock), "not due before the deadline");
        tc.advance(30);
        assert!(due(clock), "deadline fires once the clock reaches it");
    }
}
