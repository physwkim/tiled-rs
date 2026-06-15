//! TTL-expiring cache for the MongoDB adapters' lazily-loaded child maps.
//!
//! A `MongoCatalog`'s run list and a `BlueskyRunAdapter`'s stream map are each
//! loaded with the synchronous MongoDB driver on `spawn_blocking`. The previous
//! design memoised the result in a `OnceCell` for the whole process lifetime,
//! so a run (or, for an in-progress run, a stream) added to MongoDB *after* the
//! first listing stayed invisible until the server was restarted (Mongo/M1).
//!
//! [`TtlCache`] replaces that permanent cache: the loaded value is reused only
//! while it is younger than `ttl`; the first access past the TTL reloads it.
//! Two properties carry over the guarantees the `OnceCell` gave, plus the one
//! it lacked:
//!
//! * **Bounded staleness** — the cache is never more than `ttl` behind MongoDB.
//! * **Single loader per window** — the async mutex is held across the load, so
//!   concurrent callers in the same window wait for one in-flight load instead
//!   of each starting their own (the `get_or_try_init` single-init property,
//!   now repeatable each window).
//! * **No poisoning** — a failed load leaves the previous value (or `None`)
//!   untouched and propagates the error, so the next access retries rather than
//!   serving a permanently-cached failure.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// Default cache lifetime. tiled-mongo's upstream (`databroker.mongo_normalized`)
/// has no directly comparable cache, so this is a tiled-rs port decision: 60s
/// bounds how long a freshly-ingested run/stream stays invisible while keeping
/// the per-listing MongoDB load rate low.
pub(crate) const DEFAULT_TTL: Duration = Duration::from_secs(60);

/// A cache whose stored value expires after a fixed TTL and reloads on the next
/// access. See the module docs for the invariants it upholds.
pub(crate) struct TtlCache<T> {
    ttl: Duration,
    cached: Mutex<Option<(Instant, Arc<T>)>>,
}

impl<T> TtlCache<T> {
    pub(crate) fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            cached: Mutex::new(None),
        }
    }

    /// Return the cached value if it is younger than the TTL; otherwise run
    /// `load`, store the fresh value stamped with the current instant, and
    /// return it. The mutex is held across `load`, so a second caller arriving
    /// during a reload waits for that single load rather than starting another.
    /// On `load` error the stored value is left unchanged and the error is
    /// propagated (no poisoning).
    pub(crate) async fn get_or_refresh<F, Fut, E>(&self, load: F) -> Result<Arc<T>, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let mut guard = self.cached.lock().await;
        if let Some((loaded_at, value)) = guard.as_ref()
            && loaded_at.elapsed() < self.ttl
        {
            return Ok(Arc::clone(value));
        }
        let fresh = Arc::new(load().await?);
        *guard = Some((Instant::now(), Arc::clone(&fresh)));
        Ok(fresh)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[tokio::test]
    async fn fresh_value_is_cached_within_ttl() {
        let calls = AtomicUsize::new(0);
        let cache: TtlCache<usize> = TtlCache::new(Duration::from_secs(3600));

        let a = cache
            .get_or_refresh(|| async { Ok::<_, ()>(calls.fetch_add(1, Ordering::SeqCst)) })
            .await
            .unwrap();
        let b = cache
            .get_or_refresh(|| async { Ok::<_, ()>(calls.fetch_add(1, Ordering::SeqCst)) })
            .await
            .unwrap();

        // Second access within the TTL returns the cached value; loader ran once.
        assert_eq!(*a, 0);
        assert_eq!(*b, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn expired_value_is_reloaded() {
        let calls = AtomicUsize::new(0);
        // TTL 0: every access is already past the window, so each reloads.
        // `elapsed()` is always >= 0, so `elapsed() < 0` is never true.
        let cache: TtlCache<usize> = TtlCache::new(Duration::from_secs(0));

        let a = cache
            .get_or_refresh(|| async { Ok::<_, ()>(calls.fetch_add(1, Ordering::SeqCst)) })
            .await
            .unwrap();
        let b = cache
            .get_or_refresh(|| async { Ok::<_, ()>(calls.fetch_add(1, Ordering::SeqCst)) })
            .await
            .unwrap();

        // The TTL expired between accesses, so the loader ran each time and the
        // second access observed the newer value.
        assert_eq!(*a, 0);
        assert_eq!(*b, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn failed_load_does_not_poison_cache() {
        let calls = AtomicUsize::new(0);
        let cache: TtlCache<usize> = TtlCache::new(Duration::from_secs(3600));

        // First load fails: nothing is cached.
        let r1 = cache
            .get_or_refresh(|| async { Err::<usize, &str>("boom") })
            .await;
        assert!(r1.is_err());

        // Next access retries (the failure was not memoised) and succeeds.
        let r2 = cache
            .get_or_refresh(|| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>(42)
            })
            .await;
        assert_eq!(*r2.unwrap(), 42);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
