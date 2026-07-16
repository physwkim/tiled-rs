//! `Context` — HTTP transport, auth state, and server discovery.
//!
//! Mirrors `tiled/client/context.py`. Holds the reqwest client, base URI,
//! API key OR OIDC tokens, the cached `About` payload, an optional HTTP cache
//! and an optional CSRF cookie. The Python `Context` is a generator-based
//! `httpx.Auth`; in Rust the auth integration sits on `Context` itself —
//! every request goes through `send_with_auth` which handles the
//! send → 401? → refresh → retry-once loop.

use std::sync::Arc;

use reqwest::header::{ACCEPT_ENCODING, HeaderMap, HeaderValue};
use reqwest::{Client, Method, RequestBuilder, Response};
use tokio::sync::{Mutex, RwLock};
use url::Url;

use crate::core::schemas::About;

use crate::client::any_client::ClientResolver;
use crate::client::auth::{TiledAuth, Tokens};
use crate::client::cache::HttpCache;
use crate::client::error::{ClientError, Result};
use crate::client::utils::{decode_response, default_headers, handle_error, retry};

/// Cap on concurrent connections / data fetches. Mirrors Python tiled's
/// `MAX_CONCURRENT_CONNECTIONS = 16` (`tiled/client/context.py:39`), which it
/// feeds to `httpx.Limits(max_connections=16, max_keepalive_connections=16)`.
/// reqwest's builder exposes only the keep-alive idle pool
/// (`pool_max_idle_per_host`), not httpx's hard total-`max_connections` cap.
/// Python enforces that hard cap with a separate application-level
/// `threading.Semaphore` (`context.py:297`); we mirror it with
/// [`ContextInner::data_fetch_semaphore`], acquired at the same bulk-data
/// fetch sites Python throttles.
pub(crate) const MAX_CONCURRENT_CONNECTIONS: usize = 16;

/// Connection context: HTTP client + base URL + auth state.
///
/// `Context` is cheap to clone (`Arc`-wrapped internal state) so client objects
/// can hold their own reference and be passed across tasks freely.
#[derive(Debug, Clone)]
pub struct Context {
    pub(crate) inner: Arc<ContextInner>,
}

pub(crate) struct ContextInner {
    /// Reqwest HTTP client (rustls + cookie store).
    pub(crate) http: Client,
    /// Server base URL, **without** trailing slash (e.g. `http://localhost:8000`).
    pub(crate) base_url: Url,
    /// API root (`<base_url>/api/v1/`), with trailing slash.
    pub(crate) api_uri: Url,
    /// API key for `Authorization: Apikey ...` (mutually exclusive with auth).
    pub(crate) api_key: RwLock<Option<String>>,
    /// OIDC auth state (mutually exclusive with api_key).
    pub(crate) auth: RwLock<Option<TiledAuth>>,
    /// Cached About payload from `GET /api/v1/`.
    pub(crate) server_info: tokio::sync::OnceCell<About>,
    /// CSRF cookie value, captured after the initial about fetch.
    ///
    /// `Arc` so the live value is SHARED with `TiledAuth` (the token-refresh
    /// path) rather than snapshotted: when the server rotates `tiled_csrf`,
    /// the next refresh must echo the new value as `x-csrf` (double-submit),
    /// not a stale construction-time copy. Mirrors Python, where
    /// `build_refresh_request` reads `context.csrf_token` live (auth.py:170).
    pub(crate) csrf_token: Arc<RwLock<Option<String>>>,
    /// Serialises the token-refresh network call so concurrent 401s produce
    /// exactly one refresh round-trip. Mirrors Python `_sync_lock`.
    pub(crate) refresh_lock: Mutex<()>,
    /// Optional HTTP cache.
    pub(crate) cache: Option<Arc<HttpCache>>,
    /// Optional client resolver for spec-based dispatch.
    pub(crate) resolver: Option<Arc<dyn ClientResolver>>,
    /// Hard ceiling on concurrent bulk-data fetches, shared across every
    /// client on this context. Mirrors Python's per-`Context`
    /// `_concurrent_request_semaphore` (`context.py:297`): reqwest's pool
    /// exposes no hard total-connection cap, so the ceiling is enforced here
    /// at the three fetch sites Python wraps with `throttle()` — array
    /// block/slice and dataframe partition (`array.py:133,181`,
    /// `dataframe.py:122`). Metadata/search/auth requests are not throttled,
    /// matching Python.
    pub(crate) data_fetch_semaphore: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for ContextInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Avoid leaking the api_key / csrf_token / auth state when a
        // user formats a `Context` for debugging — they'd otherwise hit
        // raw bearer credentials in a log line.
        f.debug_struct("ContextInner")
            .field("base_url", &self.base_url)
            .field("api_uri", &self.api_uri)
            .field("api_key", &"<redacted>")
            .field("auth", &"<redacted>")
            .field("csrf_token", &"<redacted>")
            .field("cache", &self.cache.as_ref().map(|_| "<set>"))
            .field("resolver", &self.resolver.as_ref().map(|_| "<set>"))
            .finish()
    }
}

/// Pull the `tiled_csrf` cookie value out of a response, if present. Single
/// owner of the double-submit-cookie parse so both [`Context::maybe_capture_csrf`]
/// and [`crate::client::auth::TiledAuth::refresh`] update the shared csrf store the
/// same way (the refresh response can itself rotate the cookie).
pub(crate) fn extract_tiled_csrf(resp: &Response) -> Option<String> {
    resp.cookies()
        .find(|c| c.name() == "tiled_csrf")
        .map(|c| c.value().to_string())
}

impl Context {
    /// Build a context from a base URL.
    pub fn from_uri(uri: &str) -> Result<(Self, Vec<String>)> {
        Self::from_uri_with_options(uri, ContextOptions::default())
    }

    /// Like [`Self::from_uri`] but accepts options.
    pub fn from_uri_with_options(
        uri: &str,
        options: ContextOptions,
    ) -> Result<(Self, Vec<String>)> {
        let mut parsed = Url::parse(uri)?;

        // Strip any `?api_key=` from the URL — promote it to a header instead.
        let mut api_key = options.api_key.clone();
        if api_key.is_none()
            && let Some((k, v)) = parsed
                .query_pairs()
                .find(|(k, _)| k == "api_key")
                .map(|(k, v)| (k.into_owned(), v.into_owned()))
        {
            api_key = Some(v);
            let remaining: Vec<(String, String)> = parsed
                .query_pairs()
                .filter(|(name, _)| name != &k)
                .map(|(a, b)| (a.into_owned(), b.into_owned()))
                .collect();
            let mut q = parsed.query_pairs_mut();
            q.clear();
            for (a, b) in &remaining {
                q.append_pair(a, b);
            }
            drop(q);
            if parsed.query() == Some("") {
                parsed.set_query(None);
            }
        }
        if api_key.is_none() {
            api_key = std::env::var("TILED_API_KEY").ok();
        }

        // Find the API root inside the URL path. Supports sub-path hosting:
        //   https://example.com/tiled/api/v1/foo/bar
        //     → api_root_path = "/tiled/api/v1/"
        //     → node_path = ["foo", "bar"]
        // If `/api/v1/` is not in the path, treat the whole path as a prefix
        // and append `/api/v1/`.
        let path = parsed.path().to_string();
        let (api_root_path, node_path_parts): (String, Vec<String>) =
            if let Some(idx) = path.find("/api/v1/") {
                let api_root_end = idx + "/api/v1/".len();
                let root = path[..api_root_end].to_string();
                let after = &path[api_root_end..];
                let parts: Vec<String> = after
                    .trim_start_matches('/')
                    .trim_end_matches('/')
                    .split('/')
                    .filter(|s| !s.is_empty())
                    .map(String::from)
                    .collect();
                (root, parts)
            } else {
                let prefix = path.trim_end_matches('/');
                let root = if prefix.is_empty() {
                    "/api/v1/".to_string()
                } else {
                    format!("{prefix}/api/v1/")
                };
                (root, Vec::new())
            };

        let mut api_uri = parsed.clone();
        api_uri.set_path(&api_root_path);
        api_uri.set_query(None);
        api_uri.set_fragment(None);

        let mut base_url = parsed.clone();
        base_url.set_path("");
        base_url.set_query(None);
        base_url.set_fragment(None);

        let http = match options.http_client {
            Some(c) => {
                // We can't introspect whether the user-supplied Client has
                // the cookie store enabled. Warn so misconfigured clients
                // don't silently fail tiled's double-submit-cookie CSRF
                // pattern (which needs both Cookie + x-csrf header).
                tracing::warn!(
                    target: "tiled.client",
                    "ContextOptions::http_client supplied; ensure cookie_store(true) is enabled or POST/PATCH/DELETE will fail CSRF checks"
                );
                c
            }
            None => Client::builder()
                .user_agent(crate::client::utils::USER_AGENT_VALUE)
                .cookie_store(true)
                .pool_max_idle_per_host(MAX_CONCURRENT_CONNECTIONS)
                .build()?,
        };

        let ctx = Self {
            inner: Arc::new(ContextInner {
                http,
                base_url,
                api_uri,
                api_key: RwLock::new(api_key),
                auth: RwLock::new(None),
                server_info: tokio::sync::OnceCell::new(),
                csrf_token: Arc::new(RwLock::new(None)),
                refresh_lock: Mutex::new(()),
                cache: options.cache,
                resolver: options.resolver,
                data_fetch_semaphore: Arc::new(tokio::sync::Semaphore::new(
                    MAX_CONCURRENT_CONNECTIONS,
                )),
            }),
        };
        Ok((ctx, node_path_parts))
    }

    pub fn http(&self) -> &Client {
        &self.inner.http
    }

    pub fn base_url(&self) -> &Url {
        &self.inner.base_url
    }

    pub fn api_uri(&self) -> &Url {
        &self.inner.api_uri
    }

    pub async fn api_key(&self) -> Option<String> {
        self.inner.api_key.read().await.clone()
    }

    pub async fn set_api_key(&self, key: Option<String>) {
        *self.inner.api_key.write().await = key;
    }

    pub async fn auth(&self) -> Option<TiledAuth> {
        self.inner.auth.read().await.clone()
    }

    pub async fn set_auth(&self, auth: Option<TiledAuth>) -> Result<()> {
        if auth.is_some() && self.api_key().await.is_some() {
            return Err(ClientError::Invalid(
                "cannot set OIDC auth while api_key is configured".into(),
            ));
        }
        *self.inner.auth.write().await = auth;
        Ok(())
    }

    pub async fn csrf_token(&self) -> Option<String> {
        self.inner.csrf_token.read().await.clone()
    }

    /// Whether this context is authenticated (api_key OR oidc).
    pub async fn authenticated(&self) -> bool {
        self.api_key().await.is_some() || self.auth().await.is_some()
    }

    /// Optional cache, if configured.
    pub fn cache(&self) -> Option<&Arc<HttpCache>> {
        self.inner.cache.as_ref()
    }

    /// Optional client resolver for spec-based dispatch. Cheap to call —
    /// returns a clone of the internal `Arc`.
    pub fn resolver(&self) -> Option<Arc<dyn ClientResolver>> {
        self.inner.resolver.as_ref().map(Arc::clone)
    }

    // ---------------- Request orchestration ----------------

    async fn auth_header_value(&self) -> Result<Option<HeaderValue>> {
        if let Some(key) = self.api_key().await {
            return Ok(Some(
                HeaderValue::from_str(&format!("Apikey {key}"))
                    .map_err(|e| ClientError::Invalid(format!("invalid api key: {e}")))?,
            ));
        }
        if let Some(auth) = self.auth().await
            && let Some(h) = auth.auth_header().await
        {
            return Ok(Some(HeaderValue::from_str(&h).map_err(|e| {
                ClientError::Invalid(format!("invalid bearer: {e}"))
            })?));
        }
        Ok(None)
    }

    async fn build_default_headers(&self) -> Result<HeaderMap> {
        let mut headers = default_headers(None)?;
        if let Some(v) = self.auth_header_value().await? {
            headers.insert(reqwest::header::AUTHORIZATION, v);
        }
        Ok(headers)
    }

    /// Send a pre-built request, transparently refreshing OIDC tokens on 401.
    ///
    /// On 401 the function implements the single-flight refresh pattern
    /// (mirrors Python `sync_auth_flow` + `_sync_lock` in `tiled/client/auth.py`):
    ///
    /// 1. Extract the `Authorization` value from the failing request.
    /// 2. Re-read the stored access token.  If it already differs from what
    ///    was sent (another task refreshed concurrently), skip the network call.
    /// 3. Otherwise acquire `refresh_lock` so only one task posts to the token
    ///    endpoint.  Waiters re-check after the lock; if the lock-holder saved a
    ///    new token they skip the call too.
    /// 4. Retry the request with the now-current `Authorization` header.
    pub async fn send_with_auth(&self, req: RequestBuilder) -> Result<Response> {
        let req_clone = req.try_clone().ok_or_else(|| {
            ClientError::Invalid("request body not cloneable; cannot retry".into())
        })?;
        let resp = req_clone.send().await?;
        if resp.status() != 401 {
            return Ok(resp);
        }
        let auth = self.auth().await;
        let Some(a) = auth else {
            // api_key or unauthenticated — nothing to refresh.
            return Ok(resp);
        };

        // What Authorization value was in the request that just 401'd?
        let used_auth: Option<String> =
            req.try_clone().and_then(|b| b.build().ok()).and_then(|r| {
                r.headers()
                    .get(reqwest::header::AUTHORIZATION)
                    .and_then(|v| v.to_str().ok())
                    .map(String::from)
            });

        // What does the token store hold right now?
        let current_auth = a.auth_header().await;

        if current_auth.as_deref() != used_auth.as_deref() {
            // A concurrent task already refreshed; the new token is in the
            // store.  Fall through and retry without a network round-trip.
        } else {
            // Token is still stale.  Acquire the per-context lock so only one
            // task performs the network refresh.  Waiters re-check after the
            // lock-holder saves new tokens and releases.
            let _guard = self.inner.refresh_lock.lock().await;
            let current_after_lock = a.auth_header().await;
            if current_after_lock.as_deref() == used_auth.as_deref() {
                // Still stale — we hold the lock; perform the refresh.
                a.refresh(&self.inner.http).await?;
            }
            // else: another waiter refreshed while we waited for the lock.
        }

        // Retry the original request with the now-current auth header,
        // replacing the stale one to avoid a duplicate Authorization header.
        let mut request = req
            .try_clone()
            .ok_or_else(|| ClientError::Invalid("request body not cloneable on retry".into()))?
            .build()
            .map_err(ClientError::from)?;
        request.headers_mut().remove(reqwest::header::AUTHORIZATION);
        if let Some(v) = self.auth_header_value().await? {
            request
                .headers_mut()
                .insert(reqwest::header::AUTHORIZATION, v);
        }
        Ok(self.inner.http.execute(request).await?)
    }

    /// Build a request with default headers + auth and return the builder so
    /// callers can add their own headers / body / query.
    pub async fn request(&self, method: Method, url: &Url) -> Result<RequestBuilder> {
        let headers = self.build_default_headers().await?;
        Ok(self
            .inner
            .http
            .request(method, url.as_str())
            .headers(headers))
    }

    /// Send a GET, applying default headers + auth and the msgpack Accept.
    pub async fn get(&self, url: &Url) -> Result<Response> {
        self.get_with_accept(url, crate::client::utils::MSGPACK_MIME_TYPE)
            .await
    }

    /// Send a GET with a caller-chosen Accept. Cache lookup keys by
    /// `(url, accept)`.
    pub async fn get_with_accept(&self, url: &Url, accept: &str) -> Result<Response> {
        if let Some(cache) = self.cache()
            && let Some(cached) = cache.try_get(url, accept).await?
        {
            return Ok(cached);
        }
        let mut req = self.request(Method::GET, url).await?;
        if let Some(cache) = self.cache() {
            let cond = cache.conditional_headers(url, accept).await?;
            if !cond.is_empty() {
                req = req.headers(cond);
            }
        }
        let req = req.header(reqwest::header::ACCEPT, accept);
        let resp = self.send_with_auth(req).await?;
        // Capture CSRF before potentially erroring on non-2xx — the server
        // sometimes rotates the cookie on auth failure.
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        if resp.status().as_u16() == 304 {
            // Server says "your cached copy is still good".
            if let Some(cache) = self.cache()
                && let Some(refreshed) = cache.revalidate_existing(url, accept, &resp).await?
            {
                return Ok(refreshed);
            }
            // No cache or no entry — surface a clearer error than a bare 304.
            return Err(ClientError::Invalid(
                "server returned 304 but no cached entry exists".into(),
            ));
        }
        if let Some(cache) = self.cache() {
            let (rebuilt, _bytes) = cache.store_response(url, accept, resp).await?;
            return Ok(rebuilt);
        }
        Ok(resp)
    }

    pub async fn get_json<T>(&self, url: &Url) -> Result<crate::core::schemas::Response<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let resp = self.get(url).await?;
        decode_response::<crate::core::schemas::Response<T>>(resp).await
    }

    /// Acquire a permit before a bulk-data fetch, capping concurrent
    /// data-fetch GETs at `MAX_CONCURRENT_CONNECTIONS`. Mirrors Python
    /// `Context.throttle()` (`context.py:661`): the permit is held across the
    /// fetch (including retries) and released on drop. The semaphore is never
    /// closed, so acquisition cannot fail.
    pub(crate) async fn data_fetch_permit(&self) -> tokio::sync::OwnedSemaphorePermit {
        Arc::clone(&self.inner.data_fetch_semaphore)
            .acquire_owned()
            .await
            .expect("data-fetch semaphore is never closed")
    }

    pub async fn get_bytes(&self, url: &Url, accept: &str) -> Result<bytes::Bytes> {
        if let Some(cache) = self.cache()
            && let Some(cached) = cache.try_get(url, accept).await?
        {
            return Ok(cached.bytes().await?);
        }
        let mut req = self.request(Method::GET, url).await?;
        if let Some(cache) = self.cache() {
            let cond = cache.conditional_headers(url, accept).await?;
            if !cond.is_empty() {
                req = req.headers(cond);
            }
        }
        let req = req.header(reqwest::header::ACCEPT, accept).header(
            ACCEPT_ENCODING,
            crate::client::blosc2::ACCEPT_ENCODING_BLOSC2,
        );
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;

        // Read Content-Encoding before the body is consumed.
        let is_blosc2 = resp
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("blosc2"))
            .unwrap_or(false);

        if resp.status().as_u16() == 304 {
            if let Some(cache) = self.cache()
                && let Some(refreshed) = cache.revalidate_existing(url, accept, &resp).await?
            {
                return Ok(refreshed.bytes().await?);
            }
            return Err(ClientError::Invalid(
                "server returned 304 but no cached entry exists".into(),
            ));
        }

        // Blosc2 responses: decode without caching to avoid storing compressed
        // bytes under an unencoded cache key (which would confuse callers that
        // don't send Accept-Encoding: blosc2 but hit the same cache entry).
        if is_blosc2 {
            let bytes = resp.bytes().await?;
            return crate::client::blosc2::decompress(&bytes);
        }

        if let Some(cache) = self.cache() {
            let (_rebuilt, bytes) = cache.store_response(url, accept, resp).await?;
            return Ok(bytes);
        }
        Ok(resp.bytes().await?)
    }

    pub async fn post_json(&self, url: &Url, body: &serde_json::Value) -> Result<Response> {
        let req = self.request(Method::POST, url).await?.json(body);
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        // A successful write may invalidate the cached GET for this URL.
        if let Some(cache) = self.cache() {
            cache.invalidate(url).await?;
        }
        Ok(resp)
    }

    /// POST a JSON body and return the raw response bytes, with an explicit
    /// `Accept`. Used by the wide-table read fallback (`dataframe.py:122-133`):
    /// when a column projection would overflow the GET URL, the columns move
    /// into a JSON body and the data still comes back as Arrow IPC bytes. This
    /// is a read, so it neither consults nor invalidates the response cache.
    pub async fn post_bytes(
        &self,
        url: &Url,
        accept: &str,
        body: &serde_json::Value,
    ) -> Result<bytes::Bytes> {
        let req = self
            .request(Method::POST, url)
            .await?
            .header(reqwest::header::ACCEPT, accept)
            .header(
                ACCEPT_ENCODING,
                crate::client::blosc2::ACCEPT_ENCODING_BLOSC2,
            )
            .json(body);
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;

        let is_blosc2 = resp
            .headers()
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("blosc2"))
            .unwrap_or(false);

        let bytes = resp.bytes().await?;
        if is_blosc2 {
            crate::client::blosc2::decompress(&bytes)
        } else {
            Ok(bytes)
        }
    }

    pub async fn patch_json(&self, url: &Url, body: &serde_json::Value) -> Result<Response> {
        let req = self.request(Method::PATCH, url).await?.json(body);
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        if let Some(cache) = self.cache() {
            cache.invalidate(url).await?;
        }
        Ok(resp)
    }

    /// PUT a JSON body, e.g. a wholesale metadata replacement
    /// (`PUT /api/v1/metadata/{path}`).
    pub async fn put_json(&self, url: &Url, body: &serde_json::Value) -> Result<Response> {
        let req = self.request(Method::PUT, url).await?.json(body);
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        if let Some(cache) = self.cache() {
            cache.invalidate(url).await?;
        }
        Ok(resp)
    }

    /// PUT or PATCH a raw-bytes body; shared by array/table write paths. When
    /// `content_type` is `Some`, it is set as the `Content-Type` header (the
    /// ragged write paths send `application/zip`); array/table writes leave it
    /// unset (raw octet-stream / Arrow IPC the server reads positionally).
    async fn bytes_write(
        &self,
        method: Method,
        url: &Url,
        body: bytes::Bytes,
        content_type: Option<&str>,
    ) -> Result<Response> {
        let mut req = self.request(method, url).await?.body(body);
        if let Some(ct) = content_type {
            req = req.header(reqwest::header::CONTENT_TYPE, ct);
        }
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        if let Some(cache) = self.cache() {
            cache.invalidate(url).await?;
        }
        Ok(resp)
    }

    pub async fn put_bytes(&self, url: &Url, body: bytes::Bytes) -> Result<Response> {
        self.bytes_write(Method::PUT, url, body, None).await
    }

    pub async fn patch_bytes(&self, url: &Url, body: bytes::Bytes) -> Result<Response> {
        self.bytes_write(Method::PATCH, url, body, None).await
    }

    /// PUT a raw-bytes body with an explicit `Content-Type` (the ragged write
    /// paths send `application/zip`).
    pub async fn put_bytes_typed(
        &self,
        url: &Url,
        body: bytes::Bytes,
        content_type: &str,
    ) -> Result<Response> {
        self.bytes_write(Method::PUT, url, body, Some(content_type))
            .await
    }

    /// PATCH a raw-bytes body with an explicit `Content-Type`.
    pub async fn patch_bytes_typed(
        &self,
        url: &Url,
        body: bytes::Bytes,
        content_type: &str,
    ) -> Result<Response> {
        self.bytes_write(Method::PATCH, url, body, Some(content_type))
            .await
    }

    pub async fn delete(&self, url: &Url) -> Result<Response> {
        let req = self.request(Method::DELETE, url).await?;
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        if let Some(cache) = self.cache() {
            cache.invalidate(url).await?;
        }
        Ok(resp)
    }

    pub(crate) async fn add_csrf(&self, req: RequestBuilder) -> RequestBuilder {
        match self.csrf_token().await {
            Some(t) => req.header("x-csrf", t),
            None => req,
        }
    }

    pub(crate) async fn maybe_capture_csrf(&self, resp: &Response) {
        // `cookie_store(true)` makes the Client persist cookies for the next
        // request automatically. We additionally snapshot `tiled_csrf` here
        // so we can echo it as the `x-csrf` header on POST/PATCH/DELETE
        // (double-submit-cookie pattern). Always overwrite — if the server
        // rotates the cookie (e.g. after a session refresh), we must pick up
        // the new value or every subsequent write will 401.
        if let Some(value) = extract_tiled_csrf(resp) {
            *self.inner.csrf_token.write().await = Some(value);
        }
    }

    pub async fn server_info(&self) -> Result<&About> {
        let inner = &self.inner;
        let info = inner
            .server_info
            .get_or_try_init(|| async {
                retry(|| async {
                    let resp = self.get(&inner.api_uri).await?;
                    decode_response::<About>(resp).await
                })
                .await
            })
            .await?;
        Ok(info)
    }

    // ---------------- Auth flows ----------------

    /// Interactive login. Mirrors `Context.authenticate`.
    ///
    /// Reads `About.authentication.providers`, walks the user through
    /// provider selection, calls `password_grant` or `device_code_grant`, and
    /// stores the resulting tokens (on disk if `remember_me`).
    pub async fn authenticate(&self, remember_me: bool) -> Result<()> {
        if self.api_key().await.is_some() {
            return Err(ClientError::Invalid(
                "cannot authenticate via OIDC while api_key is set".into(),
            ));
        }
        let info = self.server_info().await?.clone();
        let providers: Vec<crate::client::auth::AuthProvider> = info
            .authentication
            .providers
            .iter()
            .filter_map(|v| {
                crate::client::auth::AuthProvider::from_json(v, Some(&self.inner.api_uri)).ok()
            })
            .collect();
        if providers.is_empty() {
            return Err(ClientError::AuthRequired(
                "server has no authentication providers".into(),
            ));
        }
        let tokens =
            crate::client::auth::prompt_for_credentials(&self.inner.http, &providers).await?;
        self.configure_auth(tokens, remember_me).await
    }

    /// Wire up an authenticator with already-obtained tokens.
    pub async fn configure_auth(&self, tokens: Tokens, remember_me: bool) -> Result<()> {
        if self.api_key().await.is_some() {
            return Err(ClientError::Invalid(
                "cannot configure auth while api_key is set".into(),
            ));
        }
        let info = self.server_info().await?.clone();
        let refresh_url = info
            .authentication
            .links
            .as_ref()
            .and_then(|l| l.get("refresh_session").and_then(|v| v.as_str()))
            .ok_or_else(|| {
                ClientError::Invalid(
                    "server does not advertise authentication.links.refresh_session".into(),
                )
            })?;
        let refresh_url = self.resolve_link(refresh_url)?;

        let csrf = self.csrf_token().await.unwrap_or_default();
        let client_id_pre = self.client_id_from_info(&info);
        if csrf.is_empty() && client_id_pre.is_none() {
            return Err(ClientError::Invalid(
                "no tiled_csrf cookie captured yet — call server_info() first or use OIDC client_id mode".into(),
            ));
        }

        let token_dir = if remember_me {
            Some(crate::client::auth::token_directory_for_server(
                &self.inner.api_uri,
            ))
        } else {
            None
        };
        let client_id = info
            .authentication
            .providers
            .first()
            .and_then(|p| p.get("links"))
            .and_then(|l| l.get("client_id"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let auth = if let Some(dir) = token_dir {
            TiledAuth::new(
                refresh_url,
                self.inner.csrf_token.clone(),
                Some(dir),
                client_id,
            )
            .await?
        } else {
            TiledAuth::in_memory(refresh_url, self.inner.csrf_token.clone(), client_id)
        };
        auth.save_tokens(&tokens).await?;
        self.set_auth(Some(auth)).await?;
        Ok(())
    }

    /// Try to use cached tokens from disk for this server.
    pub async fn use_cached_tokens(&self) -> Result<bool> {
        let info = self.server_info().await?.clone();
        let refresh_url = info
            .authentication
            .links
            .as_ref()
            .and_then(|l| l.get("refresh_session").and_then(|v| v.as_str()));
        let Some(refresh_url) = refresh_url else {
            return Ok(false);
        };
        let refresh_url = self.resolve_link(refresh_url)?;
        let dir = crate::client::auth::token_directory_for_server(&self.inner.api_uri);
        if !dir.exists() {
            return Ok(false);
        }
        let client_id = info
            .authentication
            .providers
            .first()
            .and_then(|p| p.get("links"))
            .and_then(|l| l.get("client_id"))
            .and_then(|v| v.as_str())
            .map(String::from);
        let auth = TiledAuth::new(
            refresh_url,
            self.inner.csrf_token.clone(),
            Some(dir),
            client_id,
        )
        .await?;
        // Probe with whoami; success means tokens are valid.
        self.set_auth(Some(auth)).await?;
        match self.whoami().await {
            Ok(_) => Ok(true),
            Err(_) => {
                self.set_auth(None).await?;
                Ok(false)
            }
        }
    }

    /// Identify the currently-authenticated user/service.
    pub async fn whoami(&self) -> Result<crate::client::auth::WhoAmI> {
        let info = self.server_info().await?.clone();
        let url = info
            .authentication
            .links
            .as_ref()
            .and_then(|l| l.get("whoami").and_then(|v| v.as_str()))
            .ok_or_else(|| {
                ClientError::Invalid("server does not advertise authentication.links.whoami".into())
            })?;
        let url = self.resolve_link(url)?;
        let resp = self.get(&url).await?;
        decode_response::<crate::core::schemas::Response<crate::client::auth::WhoAmI>>(resp)
            .await
            .and_then(|env| {
                env.data
                    .ok_or_else(|| ClientError::Invalid("whoami missing data".into()))
            })
    }

    /// Log out: revoke the session server-side and clear local tokens.
    ///
    /// Uses `server_info.authentication.links.logout` (OIDC client-id mode)
    /// when available, otherwise falls back to
    /// `<api_uri>auth/session/revoke` (Tiled-native mode).
    pub async fn logout(&self) -> Result<()> {
        let auth = self.auth().await;
        let Some(auth) = auth else { return Ok(()) };
        let info = self.server_info().await?.clone();
        let logout_link = info
            .authentication
            .links
            .as_ref()
            .and_then(|l| l.get("logout").and_then(|v| v.as_str()));
        let revoke_link = info
            .authentication
            .links
            .as_ref()
            .and_then(|l| l.get("revoke_session").and_then(|v| v.as_str()));

        let id_token = auth.tokens().get("id_token", false).await?;
        let refresh_token = auth.tokens().get("refresh_token", false).await?;

        if let (Some(logout_url), Some(client_id), Some(id_tok)) = (
            logout_link,
            self.client_id_from_info(&info),
            id_token.as_ref(),
        ) {
            // OIDC client-id flow: GET logout endpoint with id_token_hint.
            if let Ok(mut url) = self.resolve_link(logout_url) {
                url.query_pairs_mut()
                    .append_pair("id_token_hint", id_tok)
                    .append_pair("client_id", &client_id);
                let _ = self.get(&url).await;
            }
        } else if let Some(rt) = refresh_token {
            // Tiled-native: POST refresh_token to revoke endpoint.
            let url = if let Some(rl) = revoke_link {
                self.resolve_link(rl)?
            } else {
                let s = format!("{}auth/session/revoke", self.inner.api_uri);
                Url::parse(&s)?
            };
            let body = serde_json::json!({"refresh_token": rt});
            let _ = self.post_json(&url, &body).await;
        }

        auth.tokens().clear("access_token").await?;
        auth.tokens().clear("refresh_token").await?;
        auth.tokens().clear("id_token").await?;
        self.set_auth(None).await?;
        Ok(())
    }

    fn client_id_from_info(&self, info: &About) -> Option<String> {
        info.authentication
            .providers
            .first()
            .and_then(|p| p.get("links"))
            .and_then(|l| l.get("client_id"))
            .and_then(|v| v.as_str())
            .map(String::from)
    }

    /// Resolve a server-supplied link, which may be absolute or relative to
    /// the API root.
    fn resolve_link(&self, link: &str) -> Result<Url> {
        if let Ok(u) = Url::parse(link) {
            return Ok(u);
        }
        // Treat as relative; join against api_uri.
        self.inner
            .api_uri
            .join(link.trim_start_matches('/'))
            .map_err(ClientError::from)
    }

    // ---------------- API-key management ----------------

    /// A "who am I" for API keys: metadata about the key that authenticated
    /// the current request (`GET /api/v1/auth/apikey`).
    ///
    /// Mirrors Python `Context.which_api_key` (`context.py:825`). The server
    /// returns 401 (surfaced as [`ClientError::AuthRequired`]) when the request
    /// was not authenticated with an API key.
    pub async fn which_api_key(&self) -> Result<ApiKeyInfo> {
        let url = self.inner.api_uri.join("auth/apikey")?;
        let req = self.request(Method::GET, &url).await?.header(
            reqwest::header::ACCEPT,
            crate::client::utils::JSON_MIME_TYPE,
        );
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        decode_response::<ApiKeyInfo>(resp).await
    }

    /// Generate a new API key (`POST /api/v1/auth/apikeys`).
    ///
    /// Mirrors Python `Context.create_api_key` (`context.py:840`). The Rust
    /// server takes the lifetime as an integer number of seconds
    /// (`expires_in_seconds`) rather than Python's `expires_in` duration
    /// string, and has no `access_tags` field. `scopes = None` grants the key
    /// the caller's own scopes; a non-`None` list must be a subset of them.
    /// The returned [`ApiKeyCreated::secret`] is shown exactly once.
    pub async fn create_api_key(
        &self,
        scopes: Option<Vec<String>>,
        expires_in_seconds: Option<i64>,
        note: Option<String>,
    ) -> Result<ApiKeyCreated> {
        let url = self.inner.api_uri.join("auth/apikeys")?;
        let body = serde_json::json!({
            "scopes": scopes,
            "expires_in_seconds": expires_in_seconds,
            "note": note,
        });
        let req = self.request(Method::POST, &url).await?.json(&body);
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        decode_response::<ApiKeyCreated>(resp).await
    }

    /// Revoke an API key by its first eight characters
    /// (`DELETE /api/v1/auth/apikeys/{first_eight}`).
    ///
    /// Mirrors Python `Context.revoke_api_key` (`context.py:889`): the key must
    /// belong to the currently-authenticated principal (or the caller must hold
    /// admin scopes). The Rust server takes `first_eight` as a path segment
    /// rather than Python's query parameter. As in Python, any characters past
    /// the first eight are truncated.
    pub async fn revoke_api_key(&self, first_eight: &str) -> Result<()> {
        let first_eight = first_eight.get(..8).unwrap_or(first_eight);
        let url = self
            .inner
            .api_uri
            .join(&format!("auth/apikeys/{first_eight}"))?;
        let req = self.request(Method::DELETE, &url).await?;
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        handle_error(resp).await?;
        Ok(())
    }

    // ---------------- Session management ----------------

    /// Revoke a session by its UUID
    /// (`DELETE /api/v1/auth/session/revoke/{session_id}`).
    ///
    /// Mirrors Python `Context.revoke_session` (`context.py:1193`): the caller
    /// must be authenticated and own the session. The Rust server answers 404
    /// (surfaced as [`ClientError::Server`] with `status == 404`) when the
    /// session does not exist or belongs to another principal — the response is
    /// deliberately opaque about which. Once revoked, refresh tokens for that
    /// session stop working.
    pub async fn revoke_session(&self, session_id: &str) -> Result<()> {
        let url = self
            .inner
            .api_uri
            .join(&format!("auth/session/revoke/{session_id}"))?;
        let req = self.request(Method::DELETE, &url).await?;
        let req = self.add_csrf(req).await;
        let resp = self.send_with_auth(req).await?;
        self.maybe_capture_csrf(&resp).await;
        handle_error(resp).await?;
        Ok(())
    }

    // ---------------- Administrative accessor ----------------

    /// Accessor for administrative requests — principal management and
    /// per-principal API-key management.
    ///
    /// Mirrors upstream `Context.admin` (`context.py:331`), which groups the
    /// admin-only endpoints under a sub-object. The returned
    /// [`Admin`](crate::client::admin::Admin) borrows this context; each call
    /// still requires the caller to hold the relevant
    /// admin scope server-side (`read:principals`, `write:principals`,
    /// `admin:apikeys`).
    pub fn admin(&self) -> crate::client::admin::Admin<'_> {
        crate::client::admin::Admin::new(self)
    }
}

/// Secret + metadata returned by [`Context::create_api_key`]
/// (`POST /api/v1/auth/apikeys`). The `secret` is the full API key, returned
/// only once at creation time.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ApiKeyCreated {
    /// The full API key material (shown only at creation).
    pub secret: String,
    /// First eight characters of the key, used to identify it for revocation.
    pub first_eight: String,
    /// Scopes granted to the key.
    pub scopes: Vec<String>,
    /// Expiration instant, or `None` if the key never expires.
    pub expiration_time: Option<chrono::DateTime<chrono::Utc>>,
}

/// Metadata about an existing API key returned by [`Context::which_api_key`]
/// (`GET /api/v1/auth/apikey`). No secret is included — the key material is
/// never returned after creation.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct ApiKeyInfo {
    /// Server-side row id of the key.
    pub id: i64,
    /// First eight characters of the key.
    pub first_eight: String,
    /// Human-facing description supplied at creation, if any.
    pub note: Option<String>,
    /// Scopes granted to the key.
    pub scopes: Vec<String>,
    /// Expiration instant, or `None` if the key never expires.
    pub expiration_time: Option<chrono::DateTime<chrono::Utc>>,
    /// When the key was created.
    pub time_created: chrono::DateTime<chrono::Utc>,
    /// Most recent time the key was used, if ever.
    pub latest_activity: Option<chrono::DateTime<chrono::Utc>>,
}

/// Optional inputs for `Context::from_uri_with_options`.
#[derive(Default, Clone)]
pub struct ContextOptions {
    pub api_key: Option<String>,
    pub http_client: Option<Client>,
    pub cache: Option<Arc<HttpCache>>,
    pub resolver: Option<Arc<dyn ClientResolver>>,
}

impl std::fmt::Debug for ContextOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextOptions")
            .field("api_key", &self.api_key.as_ref().map(|_| "<set>"))
            .field("http_client", &self.http_client.as_ref().map(|_| "<set>"))
            .field("cache", &self.cache.as_ref().map(|_| "<set>"))
            .field("resolver", &self.resolver.as_ref().map(|_| "<set>"))
            .finish()
    }
}

impl ContextOptions {
    pub fn api_key(mut self, key: impl Into<String>) -> Self {
        self.api_key = Some(key.into());
        self
    }

    pub fn http_client(mut self, client: Client) -> Self {
        self.http_client = Some(client);
        self
    }

    pub fn cache(mut self, cache: Arc<HttpCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn resolver(mut self, resolver: Arc<dyn ClientResolver>) -> Self {
        self.resolver = Some(resolver);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uri_strips_node_path() {
        let (ctx, parts) = Context::from_uri("http://localhost:8000/api/v1/foo/bar").unwrap();
        assert_eq!(parts, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(ctx.api_uri().as_str(), "http://localhost:8000/api/v1/");
    }

    #[test]
    fn from_uri_no_node_path() {
        let (ctx, parts) = Context::from_uri("http://localhost:8000").unwrap();
        assert!(parts.is_empty());
        assert_eq!(ctx.api_uri().as_str(), "http://localhost:8000/api/v1/");
    }

    #[test]
    fn from_uri_sub_path_host() {
        let (ctx, parts) = Context::from_uri("https://example.com/tiled/api/v1/foo/bar").unwrap();
        assert_eq!(parts, vec!["foo".to_string(), "bar".to_string()]);
        assert_eq!(ctx.api_uri().as_str(), "https://example.com/tiled/api/v1/");
    }

    #[test]
    fn from_uri_sub_path_no_api_root_appends() {
        let (ctx, parts) = Context::from_uri("https://example.com/tiled").unwrap();
        assert!(parts.is_empty());
        assert_eq!(ctx.api_uri().as_str(), "https://example.com/tiled/api/v1/");
    }

    #[tokio::test]
    async fn from_uri_promotes_api_key_to_state() {
        let (ctx, _) = Context::from_uri("http://localhost:8000/?api_key=secret").unwrap();
        assert_eq!(ctx.api_key().await.as_deref(), Some("secret"));
    }

    #[tokio::test]
    async fn data_fetch_permits_cap_at_max_connections() {
        let (ctx, _) = Context::from_uri("http://localhost:8000").unwrap();
        // Boundary: fresh context starts with exactly MAX_CONCURRENT_CONNECTIONS
        // permits, the hard ceiling Python sets via `threading.Semaphore`.
        assert_eq!(
            ctx.inner.data_fetch_semaphore.available_permits(),
            MAX_CONCURRENT_CONNECTIONS
        );

        // Boundary: holding all permits drains the semaphore to zero, and the
        // next acquire would block — the cap is enforced, not advisory.
        let mut permits = Vec::new();
        for _ in 0..MAX_CONCURRENT_CONNECTIONS {
            permits.push(ctx.data_fetch_permit().await);
        }
        assert_eq!(ctx.inner.data_fetch_semaphore.available_permits(), 0);
        assert!(ctx.inner.data_fetch_semaphore.try_acquire().is_err());

        // Boundary: dropping the held permits restores the full ceiling.
        drop(permits);
        assert_eq!(
            ctx.inner.data_fetch_semaphore.available_permits(),
            MAX_CONCURRENT_CONNECTIONS
        );
    }
}
