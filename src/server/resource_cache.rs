//! Server-side resource cache — a TLRU (time-aware LRU) cache of built leaf
//! adapters.
//!
//! Ported from Python tiled's `tiled/adapters/resource_cache.py`. Upstream
//! caches the *raw* opened resource (`numpy.load(path)`, `Image.open(path)`,
//! `tifffile.TiffFile(path)`) keyed by `(factory, path)`, so a burst of
//! requests for the same file does not repeatedly re-open and re-parse it.
//! Only the read-only npy / jpeg / tiff adapters opt in upstream; the writable
//! adapters (csv, parquet, zarr, hdf5, sql, awkward) never touch it.
//!
//! ## What this port caches
//!
//! The tiled-rs port has no separate "raw resource" object — the whole leaf
//! adapter is rebuilt from the file on every request (see
//! [`crate::server::file_resolver`]). So this cache stores the built
//! [`AnyAdapter`] itself, keyed by [`CacheKey`] = the "factory identity"
//! (mimetype) + path + the rest of the build recipe (parameters, structure,
//! metadata). Every variant of `AnyAdapter` is `Arc<dyn …: Send + Sync>` and
//! `Clone`, so sharing one built adapter across concurrent requests is safe by
//! construction; a cache hit hands back an `Arc` refcount bump.
//!
//! ## Staleness (matches upstream)
//!
//! An entry lives for at most `ttu` seconds from *insertion* — accessing it
//! does **not** extend that lifetime (this is TTU, "time to use", not an idle
//! timeout). Within that window a changed-on-disk file is served from the
//! stale cached adapter, exactly as upstream accepts up to `TTU` seconds of
//! staleness for a changed file. Default TTU is 60 s.
//!
//! ## Why writable adapters are never cached
//!
//! A writable file adapter (e.g. `NpyAdapter`) writes new bytes to the file
//! but leaves its own in-memory `&self` snapshot untouched — it has no `&mut`
//! path to refresh itself. If such an adapter were cached, a server write
//! would land on disk while subsequent reads kept serving the pre-write
//! snapshot for up to `ttu` seconds. Upstream sidesteps this by only caching
//! its read-only adapters; this port does the same structurally — the resolver
//! inserts an entry only when the adapter is read-only (its backing file is
//! outside writable storage). Writability is a pure function of the path, so a
//! given path is either always cached or never cached; the two cases never mix
//! under one key.
//!
//! ## Concurrency (benign race, as upstream)
//!
//! The cache lock is never held while an adapter is built. Two concurrent
//! misses for the same key therefore both build and both insert (last write
//! wins) — the same benign duplicate-build race upstream's `with_resource_cache`
//! has (it does no single-flight either). The cost ceiling is one redundant
//! build per racing request; correctness is unaffected because every build for
//! a given key produces an equivalent adapter.

use std::collections::HashMap;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use serde_json::Value;

use crate::core::adapters::AnyAdapter;

/// Env var: cache capacity in items (upstream parity). `0` disables the cache.
const ENV_MAX_SIZE: &str = "TILED_RESOURCE_CACHE_MAX_SIZE";
/// Env var: time-to-use in seconds, a float (upstream parity).
const ENV_TTU: &str = "TILED_RESOURCE_CACHE_TTU";
/// Default capacity (items), matching upstream `DEFAULT_MAX_SIZE`.
const DEFAULT_MAX_SIZE: usize = 1024;
/// Default time-to-use in seconds, matching upstream `DEFAULT_TIME_TO_USE_SECONDS`.
const DEFAULT_TTU_SECONDS: f64 = 60.0;

/// A monotonic clock returning seconds. Boxed so tests can inject a manual
/// clock (mirroring cachetools' `timer` parameter) without waiting on wall
/// time. Aliased to keep the struct field readable (and clippy quiet about
/// type complexity).
type NowFn = Box<dyn Fn() -> f64 + Send + Sync>;

/// Cache key for a built leaf adapter.
///
/// Upstream keys by `(factory, path)` because it caches the raw resource,
/// whose contents depend only on those two. This port caches the whole built
/// adapter, so the key also carries the rest of the build recipe: two nodes
/// that point at the same file but declare a different HDF5 `dataset`, zarr
/// `array_path`, structure, or metadata build *different* adapters and must not
/// collide. `writable` is deliberately absent — writable adapters are never
/// inserted, and writability is a pure function of the path.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    /// The mimetype: the "factory identity" (which constructor builds it).
    factory: String,
    /// The backing file (or directory) path — exact, not lossily stringified.
    path: PathBuf,
    /// Canonical JSON of `(parameters, structure, metadata)`, the rest of the
    /// recipe `build_leaf_adapter` consumes. Stored as one string so the
    /// `serde_json::Value`s (which are neither `Hash` nor `Eq`-cheap) take part
    /// in the key. Both sides come from the same catalog rows, so identical
    /// inputs stringify identically.
    recipe: String,
}

impl CacheKey {
    /// Build a key from the resolver's per-request inputs. `\u{1}` (an
    /// otherwise-absent control char) separates the recipe fields so distinct
    /// field boundaries can't be forged by an adjacent field's content.
    pub fn new(
        factory: &str,
        path: &Path,
        parameters: &Value,
        structure: &Value,
        metadata: &Value,
    ) -> Self {
        Self {
            factory: factory.to_string(),
            path: path.to_path_buf(),
            recipe: format!("{parameters}\u{1}{structure}\u{1}{metadata}"),
        }
    }
}

/// A single cached entry.
struct Entry<V> {
    value: V,
    /// Absolute expiry time (seconds): `inserted_at + ttu`. Never extended on
    /// access — that is the TTU (not idle-TTL) semantics.
    expires_at: f64,
    /// Recency tick for LRU capacity eviction; bumped on every access and on
    /// insert. The entry with the smallest tick is the least recently used.
    last_used: u64,
}

/// Mutable interior of the cache.
struct Inner<K, V> {
    map: HashMap<K, Entry<V>>,
    /// Monotonically increasing recency counter (LRU ordering).
    tick: u64,
}

/// A time-aware LRU cache. Generic over key and value so the eviction /
/// expiry mechanism can be unit-tested with trivial payloads; production uses
/// [`AdapterCache`].
pub struct ResourceCache<K, V> {
    max_size: usize,
    ttu: f64,
    inner: Mutex<Inner<K, V>>,
    now: NowFn,
}

/// The concrete cache the file resolver owns: built adapters keyed by
/// [`CacheKey`].
pub type AdapterCache = ResourceCache<CacheKey, AnyAdapter>;

impl<K: Eq + Hash + Clone, V: Clone> ResourceCache<K, V> {
    /// Create a cache with an explicit capacity and time-to-use (seconds),
    /// driven by a monotonic clock. `max_size == 0` disables the cache: it
    /// never stores anything, so every lookup misses (matching upstream's
    /// `maxsize == 0` disable path).
    pub fn new(max_size: usize, ttu_seconds: f64) -> Self {
        let start = Instant::now();
        Self::with_clock(
            max_size,
            ttu_seconds,
            Box::new(move || start.elapsed().as_secs_f64()),
        )
    }

    /// Create a cache from the `TILED_RESOURCE_CACHE_*` environment variables,
    /// falling back to the upstream defaults (1024 items, 60 s).
    pub fn from_env() -> Self {
        let (max_size, ttu) = parse_config(
            std::env::var(ENV_MAX_SIZE).ok(),
            std::env::var(ENV_TTU).ok(),
        );
        Self::new(max_size, ttu)
    }

    /// Shared constructor taking an injectable clock.
    fn with_clock(max_size: usize, ttu_seconds: f64, now: NowFn) -> Self {
        Self {
            max_size,
            ttu: ttu_seconds,
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                tick: 0,
            }),
            now,
        }
    }

    /// Fetch a live (non-expired) value, bumping its LRU recency. Returns
    /// `None` on a miss or when the entry has outlived its TTU (which also
    /// drops the stale entry). Cloning `V` is cheap (an `Arc` bump for
    /// [`AnyAdapter`]).
    pub fn get(&self, key: &K) -> Option<V> {
        if self.max_size == 0 {
            return None;
        }
        let now = (self.now)();
        let mut inner = self.inner.lock().unwrap();
        match inner.map.get(key) {
            Some(e) if e.expires_at <= now => {
                inner.map.remove(key);
                return None;
            }
            Some(_) => {}
            None => return None,
        }
        inner.tick += 1;
        let tick = inner.tick;
        let e = inner.map.get_mut(key).expect("present: checked above");
        e.last_used = tick;
        Some(e.value.clone())
    }

    /// Offer a value to the cache. A no-op when the cache is disabled
    /// (`max_size == 0`), matching upstream's `if cache.maxsize:` guard. When
    /// inserting a new key at capacity, expired entries are purged first and
    /// then, if still full, the least-recently-used entry is evicted.
    pub fn insert(&self, key: K, value: V) {
        if self.max_size == 0 {
            return;
        }
        let now = (self.now)();
        let mut inner = self.inner.lock().unwrap();
        inner.tick += 1;
        let tick = inner.tick;
        if !inner.map.contains_key(&key) && inner.map.len() >= self.max_size {
            // At capacity for a new key: drop expired entries first (as
            // cachetools' TLRUCache.popitem does), then evict the LRU if the
            // purge did not free a slot.
            inner.map.retain(|_, e| e.expires_at > now);
            if inner.map.len() >= self.max_size
                && let Some(lru) = inner
                    .map
                    .iter()
                    .min_by_key(|(_, e)| e.last_used)
                    .map(|(k, _)| k.clone())
            {
                inner.map.remove(&lru);
            }
        }
        inner.map.insert(
            key,
            Entry {
                value,
                expires_at: now + self.ttu,
                last_used: tick,
            },
        );
    }

    /// Current entry count (test/introspection helper).
    #[cfg(test)]
    fn len(&self) -> usize {
        self.inner.lock().unwrap().map.len()
    }
}

/// Resolve `(max_size, ttu)` from the two env-var values (already read).
///
/// A missing variable uses the upstream default; a present-but-unparseable
/// value logs a warning and falls back to the default rather than panicking
/// the server (upstream's `int(...)` / `float(...)` would raise at import).
fn parse_config(max_size: Option<String>, ttu: Option<String>) -> (usize, f64) {
    let max_size = match max_size {
        Some(s) => s.trim().parse::<usize>().unwrap_or_else(|_| {
            tracing::warn!(
                value = %s,
                "{ENV_MAX_SIZE} is not a non-negative integer; using default {DEFAULT_MAX_SIZE}"
            );
            DEFAULT_MAX_SIZE
        }),
        None => DEFAULT_MAX_SIZE,
    };
    let ttu = match ttu {
        Some(s) => s.trim().parse::<f64>().unwrap_or_else(|_| {
            tracing::warn!(
                value = %s,
                "{ENV_TTU} is not a float; using default {DEFAULT_TTU_SECONDS}"
            );
            DEFAULT_TTU_SECONDS
        }),
        None => DEFAULT_TTU_SECONDS,
    };
    (max_size, ttu)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// A cache wired to a manual clock so TTU/eviction are deterministic and
    /// instant (mirrors cachetools' injectable `timer`).
    fn manual_cache(max_size: usize, ttu: f64) -> (ResourceCache<u32, u32>, Arc<Mutex<f64>>) {
        let clock = Arc::new(Mutex::new(0.0_f64));
        let c = clock.clone();
        let cache = ResourceCache::with_clock(max_size, ttu, Box::new(move || *c.lock().unwrap()));
        (cache, clock)
    }

    fn set_clock(clock: &Arc<Mutex<f64>>, t: f64) {
        *clock.lock().unwrap() = t;
    }

    /// The caller pattern from `with_resource_cache`: return the cached value,
    /// else build (counting builds) and insert.
    fn get_or_build(cache: &ResourceCache<u32, u32>, key: u32, builds: &mut u32) -> u32 {
        if let Some(v) = cache.get(&key) {
            return v;
        }
        *builds += 1;
        let v = key.wrapping_mul(10);
        cache.insert(key, v);
        v
    }

    #[test]
    fn hit_returns_cached_value_and_builds_once() {
        let (cache, _clock) = manual_cache(8, 60.0);
        let mut builds = 0;
        assert_eq!(get_or_build(&cache, 1, &mut builds), 10);
        assert_eq!(get_or_build(&cache, 1, &mut builds), 10);
        assert_eq!(builds, 1, "second lookup must be a hit, not a rebuild");
    }

    #[test]
    fn ttu_expiry_rebuilds() {
        let (cache, clock) = manual_cache(8, 60.0);
        let mut builds = 0;
        get_or_build(&cache, 1, &mut builds); // t=0, build #1
        set_clock(&clock, 59.9);
        get_or_build(&cache, 1, &mut builds); // still live -> hit
        assert_eq!(builds, 1);
        set_clock(&clock, 60.1);
        get_or_build(&cache, 1, &mut builds); // expired -> build #2
        assert_eq!(builds, 2);
    }

    #[test]
    fn access_does_not_extend_ttu() {
        // TTU is measured from insertion; a mid-life access must not push the
        // expiry out (distinguishes TTU from an idle timeout).
        let (cache, clock) = manual_cache(8, 60.0);
        let mut builds = 0;
        get_or_build(&cache, 1, &mut builds); // inserted at t=0
        set_clock(&clock, 55.0);
        assert!(cache.get(&1).is_some(), "still live at t=55");
        set_clock(&clock, 60.5);
        // Despite the access at t=55, the entry expires 60 s after insertion.
        assert!(cache.get(&1).is_none(), "expired 60 s after insertion");
        get_or_build(&cache, 1, &mut builds);
        assert_eq!(builds, 2);
    }

    #[test]
    fn capacity_evicts_least_recently_used() {
        let (cache, _clock) = manual_cache(2, 60.0);
        cache.insert(1, 11);
        cache.insert(2, 22);
        // Touch key 1 so key 2 becomes the LRU.
        assert_eq!(cache.get(&1), Some(11));
        cache.insert(3, 33); // capacity 2 -> evict LRU (key 2)
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(&2), None, "LRU key 2 evicted");
        assert_eq!(cache.get(&1), Some(11), "recently-used key 1 retained");
        assert_eq!(cache.get(&3), Some(33), "freshly inserted key 3 present");
    }

    #[test]
    fn capacity_purges_expired_before_evicting() {
        // A new key at capacity should reclaim an expired slot rather than
        // evict a live LRU entry.
        let (cache, clock) = manual_cache(2, 60.0);
        cache.insert(1, 11); // expires at 60
        set_clock(&clock, 30.0);
        cache.insert(2, 22); // expires at 90
        set_clock(&clock, 61.0); // key 1 now expired, key 2 still live
        cache.insert(3, 33); // purge expired key 1, keep live key 2
        assert_eq!(cache.get(&2), Some(22), "live key 2 survives the purge");
        assert_eq!(cache.get(&3), Some(33));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn disabled_cache_never_stores() {
        // max_size == 0 == today's behaviour: every request rebuilds.
        let (cache, _clock) = manual_cache(0, 60.0);
        let mut builds = 0;
        get_or_build(&cache, 1, &mut builds);
        get_or_build(&cache, 1, &mut builds);
        assert_eq!(builds, 2, "disabled cache must rebuild every time");
        cache.insert(1, 11);
        assert_eq!(cache.get(&1), None, "disabled cache stores nothing");
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn reinsert_same_key_keeps_one_entry() {
        // The benign concurrent-build race resolves to last-write-wins with a
        // single entry, not a leak.
        let (cache, _clock) = manual_cache(8, 60.0);
        cache.insert(1, 11);
        cache.insert(1, 111);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(&1), Some(111));
    }

    /// Compare a parsed TTU against an exactly-representable expected value
    /// without tripping `clippy::float_cmp` (a direct `==` on `f64`).
    fn ttu_is(got: f64, want: f64) -> bool {
        (got - want).abs() < f64::EPSILON
    }

    #[test]
    fn parse_config_defaults_and_overrides() {
        let (m, t) = parse_config(None, None);
        assert_eq!(m, DEFAULT_MAX_SIZE);
        assert!(ttu_is(t, DEFAULT_TTU_SECONDS));
        assert_eq!(parse_config(Some("0".into()), None).0, 0, "0 disables");
        assert_eq!(parse_config(Some("2048".into()), None).0, 2048);
        assert!(ttu_is(parse_config(None, Some("30.5".into())).1, 30.5));
        assert!(
            ttu_is(parse_config(None, Some("60.".into())).1, 60.0),
            "trailing-dot float"
        );
        // Unparseable values fall back to defaults instead of panicking.
        let (m, t) = parse_config(Some("abc".into()), Some("xyz".into()));
        assert_eq!(m, DEFAULT_MAX_SIZE);
        assert!(ttu_is(t, DEFAULT_TTU_SECONDS));
    }
}
