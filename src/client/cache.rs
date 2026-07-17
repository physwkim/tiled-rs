//! HTTP response cache with `Cache-Control` and `ETag`/`Last-Modified` validators.
//!
//! Mirrors `tiled/client/cache.py` (`Cache`) and `tiled/client/cache_control.py`
//! (`CacheControl`, validators, `apply_request_for_cache`,
//! `apply_response_for_cache`, `make_request_conditional`).
//!
//! ## Storage
//!
//! Two backends:
//!
//! - `HttpCache::in_memory(capacity_bytes)` — `HashMap` + LRU bytes accounting.
//! - `HttpCache::sqlite(path, capacity_bytes)` — persistent on-disk via sqlx.
//!   Use [`HttpCache::sqlite_with_load`] to also bootstrap the in-memory index
//!   from the existing database file.
//!
//! ## Cache-Control honored fields (request)
//!
//! - `no-cache`: skip cache, force revalidation.
//! - `no-store`: don't read or write cache.
//! - `max-age=N`, `min-fresh=N`, `max-stale[=N]`: freshness gating.
//! - `only-if-cached`: return cached or 504.
//!
//! ## Response handling
//!
//! - `no-store`: do not persist.
//! - `private`/`public`: respected — we only persist `public` or
//!   no-directive responses.
//! - `max-age=N` / `s-maxage=N`: store freshness lifetime.
//! - `Expires`: freshness lifetime when no `max-age`/`s-maxage` is present
//!   (honored only alongside a valid `Date` header).
//! - `must-revalidate`: store and force revalidate after expiry.
//! - `Vary`: keys by listed request headers.
//! - `ETag`/`Last-Modified` → `If-None-Match` / `If-Modified-Since` on
//!   conditional requests.
//! - Only responses with status in {200, 203, 300, 301, 308} are cached.
//! - Response bodies larger than `max_item_size` (default 500 KB) are not
//!   cached.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Response;
use reqwest::header::{HeaderMap, HeaderValue};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use tokio::sync::Mutex;
use url::Url;

use crate::client::error::{ClientError, Result};

/// One stored entry.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    pub url: String,
    /// The `Accept` header used on the original request. Cache lookup keys by
    /// `(url, accept)` so msgpack and JSON variants do not collide.
    pub accept: String,
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    /// When the response is considered fresh until.
    pub expires_at: DateTime<Utc>,
    /// `Cache-Control: must-revalidate` flag.
    pub must_revalidate: bool,
    /// `Cache-Control: no-cache` flag (forces revalidation).
    pub no_cache: bool,
    /// `ETag` validator, if any.
    pub etag: Option<String>,
    /// `Last-Modified` validator, if any.
    pub last_modified: Option<String>,
    /// `Vary` header tokens (lower-cased) the response advertised.
    pub vary: Vec<String>,
    /// Wall-clock time of the most recent access — set when the entry is
    /// stored, bumped on every *served* read ([`HttpCache::try_get`]), and
    /// refreshed on revalidation. This is the LRU eviction key: the
    /// least-recently-*accessed* entry is evicted first, matching upstream
    /// `Cache.get`/`Cache.set` (`tiled/client/cache.py`), which bumps
    /// `time_last_accessed` on read and orders eviction by it. Persisted in
    /// the `stored_at` column.
    pub stored_at: DateTime<Utc>,
    /// Approximate size in bytes (body + headers).
    pub size_bytes: usize,
}

/// One year — upper bound on cache freshness so a hostile server can't pin
/// an entry essentially forever via `max-age=99999999999`.
const MAX_FRESHNESS_SECS: u64 = 31_536_000;

/// Per-item body-size ceiling: response bodies larger than this are never
/// cached. Matches upstream `Cache(max_item_size=500_000)`
/// (`tiled/client/cache.py`).
const DEFAULT_MAX_ITEM_SIZE: usize = 500_000;

/// Response statuses eligible for caching, matching upstream
/// `CacheControl.cacheable_status_codes` (`tiled/client/cache_control.py`):
/// 200 OK, 203 Non-Authoritative Information, 300 Multiple Choices,
/// 301 Moved Permanently, 308 Permanent Redirect.
const CACHEABLE_STATUSES: [u16; 5] = [200, 203, 300, 301, 308];

impl CacheEntry {
    pub fn is_fresh(&self) -> bool {
        !self.no_cache && Utc::now() < self.expires_at
    }
}

/// Parsed Cache-Control directives.
#[derive(Debug, Default, Clone)]
pub struct CacheControl {
    pub no_cache: bool,
    pub no_store: bool,
    pub must_revalidate: bool,
    pub public: bool,
    pub private: bool,
    pub max_age: Option<u64>,
    pub s_maxage: Option<u64>,
    pub min_fresh: Option<u64>,
    pub max_stale: Option<Option<u64>>, // Some(None) = unbounded
    pub only_if_cached: bool,
    pub immutable: bool,
}

impl CacheControl {
    pub fn parse(value: &str) -> Self {
        let mut cc = Self::default();
        for token in value.split(',') {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            let (name, val) = match token.split_once('=') {
                Some((n, v)) => (n.to_ascii_lowercase(), Some(v.trim_matches('"'))),
                None => (token.to_ascii_lowercase(), None),
            };
            match (name.as_str(), val) {
                ("no-cache", _) => cc.no_cache = true,
                ("no-store", _) => cc.no_store = true,
                ("must-revalidate", _) => cc.must_revalidate = true,
                ("public", _) => cc.public = true,
                ("private", _) => cc.private = true,
                ("immutable", _) => cc.immutable = true,
                ("only-if-cached", _) => cc.only_if_cached = true,
                ("max-age", Some(v)) => cc.max_age = v.parse().ok(),
                ("s-maxage", Some(v)) => cc.s_maxage = v.parse().ok(),
                ("min-fresh", Some(v)) => cc.min_fresh = v.parse().ok(),
                ("max-stale", v) => {
                    cc.max_stale = Some(v.and_then(|x| x.parse().ok()));
                }
                _ => {}
            }
        }
        cc
    }
}

/// HTTP response cache. Cheaply cloneable (`Arc`-internal).
#[derive(Debug)]
pub struct HttpCache {
    backend: Mutex<Backend>,
    capacity_bytes: usize,
    /// Response bodies larger than this are declined (not cached). See
    /// [`DEFAULT_MAX_ITEM_SIZE`].
    max_item_size: usize,
    used_bytes: Mutex<usize>,
}

#[derive(Debug)]
enum Backend {
    InMemory(HashMap<String, CacheEntry>),
    Sqlite(SqliteBackend),
}

#[derive(Debug)]
struct SqliteBackend {
    path: PathBuf,
    pool: Option<SqlitePool>,
    in_memory_index: HashMap<String, CacheEntry>,
}

const SCHEMA_SQL: &str = "
    CREATE TABLE IF NOT EXISTS entries (
        url TEXT NOT NULL,
        accept TEXT NOT NULL DEFAULT '',
        status INTEGER NOT NULL,
        headers TEXT NOT NULL,
        body BLOB NOT NULL,
        expires_at TEXT NOT NULL,
        must_revalidate INTEGER NOT NULL,
        no_cache INTEGER NOT NULL,
        etag TEXT,
        last_modified TEXT,
        vary TEXT NOT NULL,
        stored_at TEXT NOT NULL,
        size_bytes INTEGER NOT NULL,
        PRIMARY KEY (url, accept)
    );
";

/// In-memory key for the cache hashmap. SQLite uses the same composite key
/// as PRIMARY KEY (url, accept).
fn cache_key(url: &Url, accept: &str) -> String {
    // Use a separator that never appears in URLs (NUL byte).
    let mut k = String::with_capacity(url.as_str().len() + accept.len() + 1);
    k.push_str(url.as_str());
    k.push('\0');
    k.push_str(accept);
    k
}

impl HttpCache {
    /// Build an in-memory cache with the default per-item size ceiling
    /// ([`DEFAULT_MAX_ITEM_SIZE`]).
    pub fn in_memory(capacity_bytes: usize) -> Arc<Self> {
        Self::in_memory_with_max_item_size(capacity_bytes, DEFAULT_MAX_ITEM_SIZE)
    }

    /// Build an in-memory cache with an explicit per-item body-size ceiling.
    /// Bodies larger than `max_item_size` are never cached, matching upstream
    /// `Cache(max_item_size=...)`.
    pub fn in_memory_with_max_item_size(capacity_bytes: usize, max_item_size: usize) -> Arc<Self> {
        debug_assert!(
            capacity_bytes > max_item_size,
            "capacity must be greater than max_item_size"
        );
        Arc::new(Self {
            backend: Mutex::new(Backend::InMemory(HashMap::new())),
            capacity_bytes,
            max_item_size,
            used_bytes: Mutex::new(0),
        })
    }

    /// Build an on-disk SQLite cache. The pool + schema are created lazily on
    /// first read or write — and on that first call we also load every
    /// existing row from disk into the in-memory index, so users do **not**
    /// need to pick between [`HttpCache::sqlite`] and
    /// [`HttpCache::sqlite_with_load`] for safety.
    /// `sqlite_with_load` remains for callers who need to surface load errors
    /// at construction time.
    pub fn sqlite(path: impl Into<PathBuf>, capacity_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            backend: Mutex::new(Backend::Sqlite(SqliteBackend {
                path: path.into(),
                pool: None,
                in_memory_index: HashMap::new(),
            })),
            capacity_bytes,
            max_item_size: DEFAULT_MAX_ITEM_SIZE,
            used_bytes: Mutex::new(0),
        })
    }

    /// Build an on-disk SQLite cache and bootstrap the in-memory index from
    /// any existing entries on disk.
    pub async fn sqlite_with_load(
        path: impl Into<PathBuf>,
        capacity_bytes: usize,
    ) -> Result<Arc<Self>> {
        let path = path.into();
        let pool = open_pool(&path).await?;
        ensure_schema(&pool).await?;
        let (entries, total) = load_all_entries(&pool).await?;
        let cache = Arc::new(Self {
            backend: Mutex::new(Backend::Sqlite(SqliteBackend {
                path,
                pool: Some(pool),
                in_memory_index: entries,
            })),
            capacity_bytes,
            max_item_size: DEFAULT_MAX_ITEM_SIZE,
            used_bytes: Mutex::new(total),
        });
        Ok(cache)
    }

    pub fn key(&self, url: &Url, accept: &str) -> String {
        cache_key(url, accept)
    }

    /// Read a cached response if fresh; otherwise return `Ok(None)`.
    ///
    /// Cache lookup keys by `(url, accept)`. If the stored entry advertised
    /// `Vary` for any header beyond `Accept`, the entry is treated as a miss
    /// (we can't verify request headers haven't drifted since we only key on
    /// URL + Accept).
    pub async fn try_get(&self, url: &Url, accept: &str) -> Result<Option<Response>> {
        self.ensure_loaded().await?;
        let key = cache_key(url, accept);
        let mut backend = self.backend.lock().await;
        let entry = match &*backend {
            Backend::InMemory(m) => m.get(&key).cloned(),
            Backend::Sqlite(b) => b.in_memory_index.get(&key).cloned(),
        };
        let Some(entry) = entry else { return Ok(None) };
        if !entry.is_fresh() {
            return Ok(None);
        }
        // Honor Vary: only allow `Accept` (we already key on it) and the
        // ubiquitous `Accept-Encoding` (handled transparently by reqwest).
        // Anything else (`Authorization`, `Cookie`, `*`, ...) means we can't
        // safely serve from cache.
        if !entry.vary.is_empty() {
            let safe = entry
                .vary
                .iter()
                .all(|v| v == "accept" || v == "accept-encoding");
            if !safe {
                return Ok(None);
            }
        }
        // Served hit: refresh the LRU recency key so eviction is truly
        // least-recently-*accessed*, not FIFO-by-insertion. Upstream
        // `Cache.get` bumps `time_last_accessed` on every hit
        // (`tiled/client/cache.py`); mirror that here and persist it for the
        // sqlite backend. We still hold the backend lock, so the in-memory
        // bump and the on-disk UPDATE cannot race a concurrent write.
        let now = Utc::now();
        let pool_for_touch = match &mut *backend {
            Backend::InMemory(m) => {
                if let Some(e) = m.get_mut(&key) {
                    e.stored_at = now;
                }
                None
            }
            Backend::Sqlite(b) => {
                if let Some(e) = b.in_memory_index.get_mut(&key) {
                    e.stored_at = now;
                }
                b.pool.clone()
            }
        };
        drop(backend);
        if let Some(pool) = pool_for_touch {
            touch_entry(&pool, url.as_str(), accept, now).await?;
        }
        let mut builder = http::Response::builder().status(entry.status);
        for (k, v) in &entry.headers {
            builder = builder.header(k, v);
        }
        let resp = builder
            .body(entry.body.clone())
            .map_err(|e| ClientError::Invalid(format!("cache build response: {e}")))?;
        Ok(Some(Response::from(resp)))
    }

    /// Bootstrap the SQLite backend on first use. No-op for in-memory.
    ///
    /// Used by callers that don't already hold the backend lock. Callers that
    /// hold it should use [`HttpCache::ensure_loaded_locked`] to avoid
    /// re-acquiring (and the resulting drop+reacquire race window).
    async fn ensure_loaded(&self) -> Result<()> {
        let mut backend = self.backend.lock().await;
        let mut used = self.used_bytes.lock().await;
        Self::ensure_loaded_locked(&mut backend, &mut used).await
    }

    /// In-place variant: caller already holds both locks.
    async fn ensure_loaded_locked(backend: &mut Backend, used: &mut usize) -> Result<()> {
        if let Backend::Sqlite(b) = backend
            && b.pool.is_none()
        {
            let pool = open_pool(&b.path).await?;
            ensure_schema(&pool).await?;
            let (entries, total) = load_all_entries(&pool).await?;
            b.in_memory_index = entries;
            *used = total;
            b.pool = Some(pool);
        }
        Ok(())
    }

    /// Store a response. The `accept` value is the `Accept` header used by the
    /// originating request; future lookups must use the same value.
    pub async fn store_response(
        &self,
        url: &Url,
        accept: &str,
        resp: Response,
    ) -> Result<(Response, Bytes)> {
        // ensure_loaded is folded into the same critical section as the
        // insert/eviction below to close a TOCTOU window where another task
        // could clear() between load and insert.
        let status = resp.status().as_u16();
        let headers: Vec<(String, String)> = resp
            .headers()
            .iter()
            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap_or("").to_string()))
            .collect();
        let cc = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("cache-control"))
            .map(|(_, v)| CacheControl::parse(v))
            .unwrap_or_default();
        // Cache only the statuses upstream deems cacheable (`CACHEABLE_STATUSES`),
        // and never a `no-store`/`private` response. 304 has no body and is
        // handled by `revalidate_existing`; it is not in the cacheable set, so
        // it is declined here.
        if cc.no_store || cc.private || !CACHEABLE_STATUSES.contains(&status) {
            let bytes = resp.bytes().await?;
            return Ok((rebuild_response(status, &headers, bytes.clone())?, bytes));
        }
        let etag = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("etag"))
            .map(|(_, v)| v.clone());
        let last_modified = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("last-modified"))
            .map(|(_, v)| v.clone());
        let vary: Vec<String> = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("vary"))
            .map(|(_, v)| {
                v.split(',')
                    .map(|s| s.trim().to_ascii_lowercase())
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now();
        let max_freshness = now + chrono::Duration::seconds(MAX_FRESHNESS_SECS as i64);
        let expires_at = match cc.max_age.or(cc.s_maxage) {
            Some(secs) => now + chrono::Duration::seconds(secs.min(MAX_FRESHNESS_SECS) as i64),
            None => match header_value(&headers, "expires") {
                // No `max-age`/`s-maxage`: honor an `Expires` header, but only
                // when a valid `Date` header is also present. Upstream
                // `is_response_fresh` (`tiled/client/cache_control.py`) returns
                // "not fresh" for an `Expires` without a parseable `Date`; with
                // both present, freshness reduces to `now <= Expires` (the Date
                // terms cancel). Clamp to the same one-year ceiling as max-age.
                Some(exp_raw) => match (
                    header_value(&headers, "date").and_then(parse_http_date),
                    parse_http_date(exp_raw),
                ) {
                    (Some(_date), Some(exp)) => exp.min(max_freshness),
                    _ => now,
                },
                // No freshness directive at all → immediately stale; the entry
                // is still stored so it can drive conditional revalidation.
                None => now,
            },
        };

        let bytes = resp.bytes().await?;
        // Per-item ceiling: decline to cache oversized bodies (compared by
        // body length, matching upstream `get_size`/`Cache.set` in
        // `tiled/client/cache.py`), but still hand the response back.
        if bytes.len() > self.max_item_size {
            return Ok((rebuild_response(status, &headers, bytes.clone())?, bytes));
        }
        let size = bytes.len()
            + headers
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>();
        let entry = CacheEntry {
            url: url.as_str().to_string(),
            accept: accept.to_string(),
            status,
            headers: headers.clone(),
            body: bytes.clone(),
            expires_at,
            must_revalidate: cc.must_revalidate,
            no_cache: cc.no_cache,
            etag,
            last_modified,
            vary,
            stored_at: Utc::now(),
            size_bytes: size,
        };
        let key = cache_key(url, accept);

        let mut backend = self.backend.lock().await;
        let mut used = self.used_bytes.lock().await;
        Self::ensure_loaded_locked(&mut backend, &mut used).await?;
        match &mut *backend {
            Backend::InMemory(m) => {
                if let Some(prev) = m.insert(key.clone(), entry.clone()) {
                    *used = used.saturating_sub(prev.size_bytes);
                }
                *used += entry.size_bytes;
            }
            Backend::Sqlite(b) => {
                if let Some(prev) = b.in_memory_index.insert(key.clone(), entry.clone()) {
                    *used = used.saturating_sub(prev.size_bytes);
                }
                *used += entry.size_bytes;
                let pool = ensure_pool(b).await?;
                upsert_entry(pool, &entry).await?;
            }
        }
        // Crude LRU. The "key" is the composite in-memory key; the disk
        // delete uses the entry's stored (url, accept) pair.
        loop {
            if *used <= self.capacity_bytes {
                break;
            }
            let evicted = match &mut *backend {
                Backend::InMemory(m) => {
                    let oldest = m
                        .iter()
                        .min_by_key(|(_, e)| e.stored_at)
                        .map(|(k, _)| k.clone());
                    if let Some(k) = oldest {
                        let prev = m.remove(&k);
                        if let Some(prev) = prev {
                            *used = used.saturating_sub(prev.size_bytes);
                        }
                        Some((k, None::<(String, String, SqlitePool)>))
                    } else {
                        None
                    }
                }
                Backend::Sqlite(b) => {
                    let oldest = b
                        .in_memory_index
                        .iter()
                        .min_by_key(|(_, e)| e.stored_at)
                        .map(|(k, _)| k.clone());
                    if let Some(k) = oldest {
                        let prev = b.in_memory_index.remove(&k);
                        if let Some(prev) = prev {
                            *used = used.saturating_sub(prev.size_bytes);
                            let url = prev.url.clone();
                            let accept = prev.accept.clone();
                            Some((k, b.pool.clone().map(|p| (url, accept, p))))
                        } else {
                            Some((k, None))
                        }
                    } else {
                        None
                    }
                }
            };
            let progressed = evicted.is_some();
            if let Some((_, Some((u, a, pool)))) = evicted {
                delete_entry(&pool, &u, &a).await?;
            }
            if !progressed {
                break;
            }
        }

        Ok((rebuild_response(status, &headers, bytes.clone())?, bytes))
    }

    /// Refresh an existing entry's freshness/validators from a 304 response,
    /// then return the cached body as a fresh `Response`. Used by
    /// `Context::get_with_accept` after the server responds 304.
    ///
    /// Returns `Ok(None)` if no matching entry exists (in which case the
    /// caller should treat the 304 as a hard error — the server thinks we
    /// have a cached copy but we don't).
    pub async fn revalidate_existing(
        &self,
        url: &Url,
        accept: &str,
        not_modified: &Response,
    ) -> Result<Option<Response>> {
        let key = cache_key(url, accept);
        let mut backend = self.backend.lock().await;
        let mut used = self.used_bytes.lock().await;
        Self::ensure_loaded_locked(&mut backend, &mut used).await?;
        drop(used); // not modifying byte count on revalidation

        let new_cc = not_modified
            .headers()
            .get(reqwest::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .map(CacheControl::parse)
            .unwrap_or_default();
        let new_etag = not_modified
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let new_last_modified = not_modified
            .headers()
            .get(reqwest::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        let new_max_age = new_cc
            .max_age
            .or(new_cc.s_maxage)
            .unwrap_or(0)
            .min(MAX_FRESHNESS_SECS);

        let entry = match &mut *backend {
            Backend::InMemory(m) => m.get_mut(&key),
            Backend::Sqlite(b) => b.in_memory_index.get_mut(&key),
        };
        let Some(entry) = entry else {
            return Ok(None);
        };
        if new_max_age > 0 {
            entry.expires_at = Utc::now() + chrono::Duration::seconds(new_max_age as i64);
        }
        if new_etag.is_some() {
            entry.etag = new_etag;
        }
        if new_last_modified.is_some() {
            entry.last_modified = new_last_modified;
        }
        entry.no_cache = new_cc.no_cache;
        entry.must_revalidate = new_cc.must_revalidate;
        entry.stored_at = Utc::now();

        // Persist the updated validators (Sqlite backend only). Hold the
        // backend lock across the upsert so a concurrent store_response or
        // invalidate can't race with our write.
        let entry_clone = entry.clone();
        let pool_for_update = match &*backend {
            Backend::Sqlite(b) => b.pool.clone(),
            _ => None,
        };
        if let Some(pool) = pool_for_update {
            upsert_entry(&pool, &entry_clone).await?;
        }
        drop(backend);

        // Build a 200-flavoured response from the entry's stored body.
        let mut builder = http::Response::builder().status(entry_clone.status);
        for (k, v) in &entry_clone.headers {
            builder = builder.header(k, v);
        }
        let resp = builder
            .body(entry_clone.body.clone())
            .map_err(|e| ClientError::Invalid(format!("revalidate response build: {e}")))?;
        Ok(Some(Response::from(resp)))
    }

    /// Build conditional revalidation headers for an entry, if one exists.
    pub async fn conditional_headers(&self, url: &Url, accept: &str) -> Result<HeaderMap> {
        self.ensure_loaded().await?;
        let mut headers = HeaderMap::new();
        let key = cache_key(url, accept);
        let backend = self.backend.lock().await;
        let entry = match &*backend {
            Backend::InMemory(m) => m.get(&key).cloned(),
            Backend::Sqlite(b) => b.in_memory_index.get(&key).cloned(),
        };
        if let Some(e) = entry {
            if let Some(etag) = e.etag
                && let Ok(v) = HeaderValue::from_str(&etag)
            {
                headers.insert(reqwest::header::IF_NONE_MATCH, v);
            }
            if let Some(lm) = e.last_modified
                && let Ok(v) = HeaderValue::from_str(&lm)
            {
                headers.insert(reqwest::header::IF_MODIFIED_SINCE, v);
            }
        }
        Ok(headers)
    }

    /// Remove every cache entry for the given URL across all `Accept` variants.
    ///
    /// Holds the backend lock across the SQL `DELETE` so a concurrent
    /// `store_response` for the same URL can't race in between.
    pub async fn invalidate(&self, url: &Url) -> Result<()> {
        self.ensure_loaded().await?;
        let url_str = url.as_str().to_string();
        let mut backend = self.backend.lock().await;
        let mut used = self.used_bytes.lock().await;
        let pool_for_delete: Option<SqlitePool> = match &*backend {
            Backend::Sqlite(b) => b.pool.clone(),
            Backend::InMemory(_) => None,
        };
        match &mut *backend {
            Backend::InMemory(m) => {
                m.retain(|_k, e| {
                    let drop_it = e.url == url_str;
                    if drop_it {
                        *used = used.saturating_sub(e.size_bytes);
                    }
                    !drop_it
                });
            }
            Backend::Sqlite(b) => {
                b.in_memory_index.retain(|_k, e| {
                    let drop_it = e.url == url_str;
                    if drop_it {
                        *used = used.saturating_sub(e.size_bytes);
                    }
                    !drop_it
                });
            }
        }
        // Issue the SQL DELETE while still holding `backend` to keep the
        // in-memory index and the on-disk table consistent.
        if let Some(pool) = pool_for_delete {
            sqlx::query("DELETE FROM entries WHERE url = ?")
                .bind(&url_str)
                .execute(&pool)
                .await
                .map_err(map_sqlx_err)?;
        }
        Ok(())
    }

    pub async fn clear(&self) -> Result<()> {
        // Initialise the SQLite pool (if not yet) so DELETE FROM entries
        // actually fires. Without this, calling clear() on a fresh sqlite()
        // cache before any other operation would leave on-disk rows that the
        // next ensure_loaded reincarnates.
        self.ensure_loaded().await?;
        let mut backend = self.backend.lock().await;
        let mut used = self.used_bytes.lock().await;
        *used = 0;
        match &mut *backend {
            Backend::InMemory(m) => m.clear(),
            Backend::Sqlite(b) => {
                b.in_memory_index.clear();
                if let Some(pool) = b.pool.clone() {
                    sqlx::query("DELETE FROM entries")
                        .execute(&pool)
                        .await
                        .map_err(map_sqlx_err)?;
                }
            }
        }
        Ok(())
    }

    pub async fn used_bytes(&self) -> usize {
        *self.used_bytes.lock().await
    }
}

fn rebuild_response(status: u16, headers: &[(String, String)], body: Bytes) -> Result<Response> {
    let mut builder = http::Response::builder().status(status);
    for (k, v) in headers {
        if let Ok(val) = HeaderValue::from_str(v) {
            builder = builder.header(k, val);
        }
    }
    let resp = builder
        .body(body)
        .map_err(|e| ClientError::Invalid(format!("cache build: {e}")))?;
    Ok(Response::from(resp))
}

/// Case-insensitive header lookup over the stored `(name, value)` pairs.
fn header_value<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case(name))
        .map(|(_, v)| v.as_str())
}

/// Parse an HTTP-date header value (`Date`, `Expires`) into UTC. HTTP dates
/// use the RFC 7231 / RFC 1123 format (`Wed, 21 Oct 2015 07:28:00 GMT`), which
/// chrono accepts via its RFC 2822 parser (including the `GMT` zone). Returns
/// `None` for anything unparseable, mirroring upstream `parse_headers_date`.
fn parse_http_date(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value.trim())
        .ok()
        .map(|d| d.with_timezone(&Utc))
}

// ---------------------------------------------------------------------------
// SQLite helpers
// ---------------------------------------------------------------------------

async fn open_pool(path: &std::path::Path) -> Result<SqlitePool> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && !parent.exists()
    {
        tokio::fs::create_dir_all(parent).await.map_err(|e| {
            ClientError::Invalid(format!("create cache dir {}: {e}", parent.display()))
        })?;
    }
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .map_err(map_sqlx_err)
}

async fn ensure_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query(SCHEMA_SQL)
        .execute(pool)
        .await
        .map_err(map_sqlx_err)?;
    Ok(())
}

async fn ensure_pool(b: &mut SqliteBackend) -> Result<&SqlitePool> {
    if b.pool.is_none() {
        let pool = open_pool(&b.path).await?;
        ensure_schema(&pool).await?;
        b.pool = Some(pool);
    }
    Ok(b.pool.as_ref().expect("just initialised"))
}

async fn upsert_entry(pool: &SqlitePool, entry: &CacheEntry) -> Result<()> {
    let headers_json = serde_json::to_string(&entry.headers)
        .map_err(|e| ClientError::Invalid(format!("encode headers: {e}")))?;
    let vary_json = serde_json::to_string(&entry.vary)
        .map_err(|e| ClientError::Invalid(format!("encode vary: {e}")))?;
    let body_blob: Vec<u8> = entry.body.to_vec();
    sqlx::query(
        "INSERT INTO entries
            (url, accept, status, headers, body, expires_at, must_revalidate, no_cache,
             etag, last_modified, vary, stored_at, size_bytes)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT(url, accept) DO UPDATE SET
            status = excluded.status,
            headers = excluded.headers,
            body = excluded.body,
            expires_at = excluded.expires_at,
            must_revalidate = excluded.must_revalidate,
            no_cache = excluded.no_cache,
            etag = excluded.etag,
            last_modified = excluded.last_modified,
            vary = excluded.vary,
            stored_at = excluded.stored_at,
            size_bytes = excluded.size_bytes",
    )
    .bind(&entry.url)
    .bind(&entry.accept)
    .bind(entry.status as i64)
    .bind(headers_json)
    .bind(body_blob)
    .bind(entry.expires_at.to_rfc3339())
    .bind(entry.must_revalidate as i64)
    .bind(entry.no_cache as i64)
    .bind(&entry.etag)
    .bind(&entry.last_modified)
    .bind(vary_json)
    .bind(entry.stored_at.to_rfc3339())
    .bind(entry.size_bytes as i64)
    .execute(pool)
    .await
    .map_err(map_sqlx_err)?;
    Ok(())
}

/// Persist a bumped LRU recency key for one entry (used by `try_get` on a
/// served hit). Matches upstream `Cache.get`'s `UPDATE ... SET
/// time_last_accessed` (`tiled/client/cache.py`).
async fn touch_entry(pool: &SqlitePool, url: &str, accept: &str, at: DateTime<Utc>) -> Result<()> {
    sqlx::query("UPDATE entries SET stored_at = ? WHERE url = ? AND accept = ?")
        .bind(at.to_rfc3339())
        .bind(url)
        .bind(accept)
        .execute(pool)
        .await
        .map_err(map_sqlx_err)?;
    Ok(())
}

async fn delete_entry(pool: &SqlitePool, url: &str, accept: &str) -> Result<()> {
    sqlx::query("DELETE FROM entries WHERE url = ? AND accept = ?")
        .bind(url)
        .bind(accept)
        .execute(pool)
        .await
        .map_err(map_sqlx_err)?;
    Ok(())
}

async fn load_all_entries(pool: &SqlitePool) -> Result<(HashMap<String, CacheEntry>, usize)> {
    use sqlx::Row;
    let rows = sqlx::query(
        "SELECT url, accept, status, headers, body, expires_at, must_revalidate, no_cache,
                etag, last_modified, vary, stored_at, size_bytes
         FROM entries",
    )
    .fetch_all(pool)
    .await
    .map_err(map_sqlx_err)?;

    let mut out = HashMap::new();
    let mut total: usize = 0;
    for row in rows {
        let url: String = row.try_get("url").map_err(map_sqlx_err)?;
        let accept: String = row.try_get("accept").map_err(map_sqlx_err)?;
        let status: i64 = row.try_get("status").map_err(map_sqlx_err)?;
        let headers_json: String = row.try_get("headers").map_err(map_sqlx_err)?;
        let body_blob: Vec<u8> = row.try_get("body").map_err(map_sqlx_err)?;
        let expires_str: String = row.try_get("expires_at").map_err(map_sqlx_err)?;
        let must_revalidate: i64 = row.try_get("must_revalidate").map_err(map_sqlx_err)?;
        let no_cache: i64 = row.try_get("no_cache").map_err(map_sqlx_err)?;
        let etag: Option<String> = row.try_get("etag").map_err(map_sqlx_err)?;
        let last_modified: Option<String> = row.try_get("last_modified").map_err(map_sqlx_err)?;
        let vary_json: String = row.try_get("vary").map_err(map_sqlx_err)?;
        let stored_str: String = row.try_get("stored_at").map_err(map_sqlx_err)?;
        let size_bytes: i64 = row.try_get("size_bytes").map_err(map_sqlx_err)?;

        let headers: Vec<(String, String)> = serde_json::from_str(&headers_json)
            .map_err(|e| ClientError::Invalid(format!("decode headers: {e}")))?;
        let vary: Vec<String> = serde_json::from_str(&vary_json)
            .map_err(|e| ClientError::Invalid(format!("decode vary: {e}")))?;
        let expires_at = DateTime::parse_from_rfc3339(&expires_str)
            .map_err(|e| ClientError::Invalid(format!("decode expires_at: {e}")))?
            .with_timezone(&Utc);
        let stored_at = DateTime::parse_from_rfc3339(&stored_str)
            .map_err(|e| ClientError::Invalid(format!("decode stored_at: {e}")))?
            .with_timezone(&Utc);

        let url_clone = url.clone();
        let accept_clone = accept.clone();
        let entry = CacheEntry {
            url,
            accept,
            status: status as u16,
            headers,
            body: Bytes::from(body_blob),
            expires_at,
            must_revalidate: must_revalidate != 0,
            no_cache: no_cache != 0,
            etag,
            last_modified,
            vary,
            stored_at,
            size_bytes: size_bytes as usize,
        };
        total += entry.size_bytes;
        let mut k = String::with_capacity(url_clone.len() + accept_clone.len() + 1);
        k.push_str(&url_clone);
        k.push('\0');
        k.push_str(&accept_clone);
        out.insert(k, entry);
    }
    Ok((out, total))
}

fn map_sqlx_err(e: sqlx::Error) -> ClientError {
    ClientError::Invalid(format!("sqlite cache: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_entry(url: &str, body: &[u8]) -> CacheEntry {
        CacheEntry {
            url: url.into(),
            accept: "application/x-msgpack".into(),
            status: 200,
            headers: vec![("content-type".into(), "text/plain".into())],
            body: Bytes::copy_from_slice(body),
            expires_at: Utc::now() + chrono::Duration::seconds(3600),
            must_revalidate: false,
            no_cache: false,
            etag: Some("\"abc\"".into()),
            last_modified: Some("Wed, 21 Oct 2026 07:28:00 GMT".into()),
            vary: vec![],
            stored_at: Utc::now(),
            size_bytes: body.len() + 32,
        }
    }

    /// Build a `reqwest::Response` for driving `store_response` in tests.
    fn make_response(status: u16, body: &[u8], headers: &[(&str, &str)]) -> Response {
        let mut builder = http::Response::builder().status(status);
        for (k, v) in headers {
            builder = builder.header(*k, *v);
        }
        Response::from(builder.body(Bytes::copy_from_slice(body)).unwrap())
    }

    const FRESH: &[(&str, &str)] = &[("cache-control", "public, max-age=3600")];

    #[test]
    fn parse_cache_control_directives() {
        let cc = CacheControl::parse("public, max-age=3600, must-revalidate");
        assert!(cc.public);
        assert_eq!(cc.max_age, Some(3600));
        assert!(cc.must_revalidate);
        assert!(!cc.no_store);
    }

    #[test]
    fn parse_no_store_directive() {
        let cc = CacheControl::parse("no-store, no-cache");
        assert!(cc.no_store);
        assert!(cc.no_cache);
    }

    #[tokio::test]
    async fn read_refreshes_lru_recency_order() {
        // Capacity holds two ~300 KB entries but not three, so storing a
        // third forces exactly one eviction. Bodies stay under the default
        // 500 KB item ceiling and capacity stays above it, so the config is
        // valid for the default `in_memory` constructor.
        let cache = HttpCache::in_memory(700_000);
        let accept = "application/json";
        let body = vec![b'x'; 300_000];
        let url_a = Url::parse("http://test/a").unwrap();
        let url_b = Url::parse("http://test/b").unwrap();
        let url_c = Url::parse("http://test/c").unwrap();

        cache
            .store_response(&url_a, accept, make_response(200, &body, FRESH))
            .await
            .unwrap();
        cache
            .store_response(&url_b, accept, make_response(200, &body, FRESH))
            .await
            .unwrap();

        // Read A: this makes A the most-recently-accessed entry, so B is now
        // the least-recently-accessed. Under FIFO-by-insertion (the bug) A
        // would still count as older than B and be evicted next.
        assert!(cache.try_get(&url_a, accept).await.unwrap().is_some());

        // Store C: must evict exactly one entry — the LRU one, which is B.
        cache
            .store_response(&url_c, accept, make_response(200, &body, FRESH))
            .await
            .unwrap();

        assert!(
            cache.try_get(&url_a, accept).await.unwrap().is_some(),
            "A was read after being stored, so it must survive eviction"
        );
        assert!(
            cache.try_get(&url_c, accept).await.unwrap().is_some(),
            "C was just stored, so it must survive eviction"
        );
        assert!(
            cache.try_get(&url_b, accept).await.unwrap().is_none(),
            "B is least-recently-accessed and must be the evicted entry"
        );
    }

    #[tokio::test]
    async fn only_whitelisted_statuses_are_cached() {
        let cache = HttpCache::in_memory(1024 * 1024);
        let accept = "application/json";

        // Every response carries a fresh max-age, so freshness never masks the
        // whitelist decision.
        for status in [200u16, 203, 300, 301, 308] {
            let url = Url::parse(&format!("http://test/ok/{status}")).unwrap();
            cache
                .store_response(&url, accept, make_response(status, b"body", FRESH))
                .await
                .unwrap();
            assert!(
                cache.try_get(&url, accept).await.unwrap().is_some(),
                "status {status} is in the cacheable set and must be cached"
            );
        }

        // 201/202/302/307 are < 400 and were cached before this fix; they are
        // not in the upstream whitelist and must now be declined.
        for status in [201u16, 202, 302, 307, 400, 404, 500] {
            let url = Url::parse(&format!("http://test/no/{status}")).unwrap();
            cache
                .store_response(&url, accept, make_response(status, b"body", FRESH))
                .await
                .unwrap();
            assert!(
                cache.try_get(&url, accept).await.unwrap().is_none(),
                "status {status} is not in the cacheable set and must not be cached"
            );
        }
    }

    #[tokio::test]
    async fn expires_header_drives_freshness() {
        let cache = HttpCache::in_memory(1024 * 1024);
        let accept = "application/json";

        let now = Utc::now();
        let date_hdr = now.to_rfc2822();
        let future = (now + chrono::Duration::seconds(3600)).to_rfc2822();
        let past = (now - chrono::Duration::seconds(3600)).to_rfc2822();

        // Future Expires + valid Date, no max-age → fresh.
        let fresh = Url::parse("http://test/fresh").unwrap();
        let fresh_hdrs = [("date", date_hdr.as_str()), ("expires", future.as_str())];
        cache
            .store_response(&fresh, accept, make_response(200, b"body", &fresh_hdrs))
            .await
            .unwrap();
        assert!(
            cache.try_get(&fresh, accept).await.unwrap().is_some(),
            "a future Expires with a valid Date must be served fresh"
        );

        // Past Expires → stale.
        let stale = Url::parse("http://test/stale").unwrap();
        let stale_hdrs = [("date", date_hdr.as_str()), ("expires", past.as_str())];
        cache
            .store_response(&stale, accept, make_response(200, b"body", &stale_hdrs))
            .await
            .unwrap();
        assert!(
            cache.try_get(&stale, accept).await.unwrap().is_none(),
            "a past Expires must be treated as stale"
        );

        // Expires present but no Date header → not fresh (upstream gate).
        let nodate = Url::parse("http://test/nodate").unwrap();
        let nodate_hdrs = [("expires", future.as_str())];
        cache
            .store_response(&nodate, accept, make_response(200, b"body", &nodate_hdrs))
            .await
            .unwrap();
        assert!(
            cache.try_get(&nodate, accept).await.unwrap().is_none(),
            "an Expires without a valid Date header must not be fresh"
        );
    }

    #[tokio::test]
    async fn max_item_size_declines_oversized_bodies() {
        // Small per-item ceiling; capacity generous so only the ceiling matters.
        let cache = HttpCache::in_memory_with_max_item_size(10_000_000, 1000);
        let accept = "application/json";

        // Body exactly at the ceiling is cached (upstream declines only when
        // strictly over: `incoming_size > max_item_size`).
        let at = Url::parse("http://test/at").unwrap();
        cache
            .store_response(&at, accept, make_response(200, &vec![b'x'; 1000], FRESH))
            .await
            .unwrap();
        assert!(
            cache.try_get(&at, accept).await.unwrap().is_some(),
            "a body of exactly max_item_size must be cached"
        );
        let used_after_at = cache.used_bytes().await;
        assert!(used_after_at > 0);

        // One byte over the ceiling: declined, but the caller still receives
        // the full body and the accounted byte count is unchanged.
        let over = Url::parse("http://test/over").unwrap();
        let (_resp, bytes) = cache
            .store_response(&over, accept, make_response(200, &vec![b'x'; 1001], FRESH))
            .await
            .unwrap();
        assert_eq!(
            bytes.len(),
            1001,
            "caller still receives the oversized body"
        );
        assert!(
            cache.try_get(&over, accept).await.unwrap().is_none(),
            "a body over max_item_size must not be cached"
        );
        assert_eq!(
            cache.used_bytes().await,
            used_after_at,
            "a declined oversized body must not change accounted bytes"
        );
    }

    #[tokio::test]
    async fn read_persists_bumped_recency_for_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        let url = Url::parse("http://test/a").unwrap();
        let accept = "application/json";

        // Each phase drops its cache/pool before the next opens one, so we
        // never hold two SQLite pools on the same file at once (avoids the
        // known cold-start pool contention flake).
        {
            let cache = HttpCache::sqlite(&path, 1024 * 1024);
            cache
                .store_response(&url, accept, make_response(200, b"hello", FRESH))
                .await
                .unwrap();
        }
        let before = load_stored_at(&path, "http://test/a", accept).await;

        {
            let cache = HttpCache::sqlite(&path, 1024 * 1024);
            assert!(cache.try_get(&url, accept).await.unwrap().is_some());
        }
        let after = load_stored_at(&path, "http://test/a", accept).await;

        assert!(
            after > before,
            "a served read must persist a later stored_at (before={before:?}, after={after:?})"
        );
    }

    async fn load_stored_at(path: &std::path::Path, url: &str, accept: &str) -> DateTime<Utc> {
        use sqlx::Row;
        let pool = open_pool(path).await.unwrap();
        let row = sqlx::query("SELECT stored_at FROM entries WHERE url = ? AND accept = ?")
            .bind(url)
            .bind(accept)
            .fetch_one(&pool)
            .await
            .unwrap();
        let s: String = row.try_get("stored_at").unwrap();
        DateTime::parse_from_rfc3339(&s)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[tokio::test]
    async fn invalidate_removes_entry_in_memory() {
        let cache = HttpCache::in_memory(1024 * 1024);
        let url = Url::parse("http://test/foo").unwrap();
        cache.invalidate(&url).await.unwrap();
        cache.clear().await.unwrap();
        assert_eq!(cache.used_bytes().await, 0);
    }

    #[tokio::test]
    async fn sqlite_cache_persists_across_instances() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");

        // Write directly via helpers.
        let pool = open_pool(&path).await.unwrap();
        ensure_schema(&pool).await.unwrap();
        let entry = dummy_entry("http://test/a", b"hello");
        upsert_entry(&pool, &entry).await.unwrap();
        drop(pool);

        // sqlite_with_load (eager): reload + assert.
        let cache = HttpCache::sqlite_with_load(&path, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(cache.used_bytes().await, entry.size_bytes);
        let url = Url::parse("http://test/a").unwrap();
        let resp = cache.try_get(&url, "application/x-msgpack").await.unwrap();
        assert!(resp.is_some());
        let body = resp.unwrap().bytes().await.unwrap();
        assert_eq!(&body[..], b"hello");

        // Invalidating removes from disk too — across all accept variants.
        cache.invalidate(&url).await.unwrap();
        let cache2 = HttpCache::sqlite_with_load(&path, 1024 * 1024)
            .await
            .unwrap();
        assert!(
            cache2
                .try_get(&url, "application/x-msgpack")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn sqlite_sync_ctor_loads_existing_entries_on_first_use() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        // Pre-populate the file out-of-band.
        let pool = open_pool(&path).await.unwrap();
        ensure_schema(&pool).await.unwrap();
        upsert_entry(&pool, &dummy_entry("http://test/a", b"x"))
            .await
            .unwrap();
        drop(pool);

        // sync ctor: should auto-load on first try_get and not silently overwrite.
        let cache = HttpCache::sqlite(&path, 1024 * 1024);
        let url = Url::parse("http://test/a").unwrap();
        let resp = cache.try_get(&url, "application/x-msgpack").await.unwrap();
        assert!(resp.is_some());
    }

    #[tokio::test]
    async fn try_get_keys_by_accept_not_just_url() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        let pool = open_pool(&path).await.unwrap();
        ensure_schema(&pool).await.unwrap();
        let mut a = dummy_entry("http://test/x", b"msgpack-body");
        a.accept = "application/x-msgpack".into();
        let mut b = dummy_entry("http://test/x", b"json-body");
        b.accept = "application/json".into();
        upsert_entry(&pool, &a).await.unwrap();
        upsert_entry(&pool, &b).await.unwrap();
        drop(pool);

        let cache = HttpCache::sqlite_with_load(&path, 1024 * 1024)
            .await
            .unwrap();
        let url = Url::parse("http://test/x").unwrap();
        let r1 = cache
            .try_get(&url, "application/x-msgpack")
            .await
            .unwrap()
            .unwrap();
        let body1 = r1.bytes().await.unwrap();
        assert_eq!(&body1[..], b"msgpack-body");
        let r2 = cache
            .try_get(&url, "application/json")
            .await
            .unwrap()
            .unwrap();
        let body2 = r2.bytes().await.unwrap();
        assert_eq!(&body2[..], b"json-body");
    }

    #[tokio::test]
    async fn sqlite_cache_clear_drops_disk_rows() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cache.sqlite");
        let pool = open_pool(&path).await.unwrap();
        ensure_schema(&pool).await.unwrap();
        for i in 0..3 {
            let e = dummy_entry(&format!("http://test/{i}"), b"xyz");
            upsert_entry(&pool, &e).await.unwrap();
        }
        drop(pool);

        let cache = HttpCache::sqlite_with_load(&path, 1024 * 1024)
            .await
            .unwrap();
        assert!(cache.used_bytes().await > 0);
        cache.clear().await.unwrap();
        assert_eq!(cache.used_bytes().await, 0);

        let cache2 = HttpCache::sqlite_with_load(&path, 1024 * 1024)
            .await
            .unwrap();
        assert_eq!(cache2.used_bytes().await, 0);
    }
}
