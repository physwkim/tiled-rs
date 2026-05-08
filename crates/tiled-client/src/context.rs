//! `Context` — HTTP transport, auth state, and server discovery.
//!
//! Mirrors `tiled/client/context.py`. Holds the reqwest client, base URI,
//! API key OR OIDC tokens, the cached `About` payload, an optional HTTP cache
//! and an optional CSRF cookie. The Python `Context` is a generator-based
//! `httpx.Auth`; in Rust the auth integration sits on `Context` itself —
//! every request goes through `send_with_auth` which handles the
//! send → 401? → refresh → retry-once loop.

use std::sync::Arc;

use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Method, RequestBuilder, Response};
use tokio::sync::RwLock;
use url::Url;

use tiled_core::schemas::About;

use crate::any_client::ClientResolver;
use crate::auth::{TiledAuth, Tokens};
use crate::cache::HttpCache;
use crate::error::{ClientError, Result};
use crate::utils::{decode_response, default_headers, handle_error, retry};

/// Connection context: HTTP client + base URL + auth state.
///
/// `Context` is cheap to clone (`Arc`-wrapped internal state) so client objects
/// can hold their own reference and be passed across tasks freely.
#[derive(Debug, Clone)]
pub struct Context {
    pub(crate) inner: Arc<ContextInner>,
}

#[derive(Debug)]
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
    pub(crate) csrf_token: RwLock<Option<String>>,
    /// Optional HTTP cache.
    pub(crate) cache: Option<Arc<HttpCache>>,
    /// Optional client resolver for spec-based dispatch.
    pub(crate) resolver: Option<Arc<dyn ClientResolver>>,
}

impl Context {
    /// Build a context from a base URL.
    pub fn from_uri(uri: &str) -> Result<(Self, Vec<String>)> {
        Self::from_uri_with_options(uri, ContextOptions::default())
    }

    /// Like [`from_uri`] but accepts options.
    pub fn from_uri_with_options(
        uri: &str,
        options: ContextOptions,
    ) -> Result<(Self, Vec<String>)> {
        let mut parsed = Url::parse(uri)?;

        // Strip any `?api_key=` from the URL — promote it to a header instead.
        let mut api_key = options.api_key.clone();
        if api_key.is_none() {
            if let Some((k, v)) = parsed
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
                .user_agent(crate::utils::USER_AGENT_VALUE)
                .cookie_store(true)
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
                csrf_token: RwLock::new(None),
                cache: options.cache,
                resolver: options.resolver,
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
        if let Some(auth) = self.auth().await {
            if let Some(h) = auth.auth_header().await {
                return Ok(Some(HeaderValue::from_str(&h).map_err(|e| {
                    ClientError::Invalid(format!("invalid bearer: {e}"))
                })?));
            }
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
    /// On 401 we refresh the token and retry once. The retry replaces the
    /// `Authorization` header (we operate on a built `Request`, not a builder
    /// — `RequestBuilder::header` would *append* a duplicate Authorization).
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
            return Ok(resp);
        };
        a.refresh(&self.inner.http).await?;

        // Build the original request, override Authorization in-place.
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
        self.get_with_accept(url, crate::utils::MSGPACK_MIME_TYPE)
            .await
    }

    /// Send a GET with a caller-chosen Accept. Cache lookup keys by
    /// `(url, accept)`.
    pub async fn get_with_accept(&self, url: &Url, accept: &str) -> Result<Response> {
        if let Some(cache) = self.cache() {
            if let Some(cached) = cache.try_get(url, accept).await? {
                return Ok(cached);
            }
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
            if let Some(cache) = self.cache() {
                if let Some(refreshed) = cache.revalidate_existing(url, accept, &resp).await? {
                    return Ok(refreshed);
                }
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

    pub async fn get_json<T>(&self, url: &Url) -> Result<tiled_core::schemas::Response<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        let resp = self.get(url).await?;
        decode_response::<tiled_core::schemas::Response<T>>(resp).await
    }

    pub async fn get_bytes(&self, url: &Url, accept: &str) -> Result<bytes::Bytes> {
        if let Some(cache) = self.cache() {
            if let Some(cached) = cache.try_get(url, accept).await? {
                return Ok(cached.bytes().await?);
            }
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
        self.maybe_capture_csrf(&resp).await;
        let resp = handle_error(resp).await?;
        if resp.status().as_u16() == 304 {
            if let Some(cache) = self.cache() {
                if let Some(refreshed) = cache.revalidate_existing(url, accept, &resp).await? {
                    return Ok(refreshed.bytes().await?);
                }
            }
            return Err(ClientError::Invalid(
                "server returned 304 but no cached entry exists".into(),
            ));
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

    async fn add_csrf(&self, req: RequestBuilder) -> RequestBuilder {
        match self.csrf_token().await {
            Some(t) => req.header("x-csrf", t),
            None => req,
        }
    }

    async fn maybe_capture_csrf(&self, resp: &Response) {
        // `cookie_store(true)` makes the Client persist cookies for the next
        // request automatically. We additionally snapshot `tiled_csrf` here
        // so we can echo it as the `x-csrf` header on POST/PATCH/DELETE
        // (double-submit-cookie pattern). Always overwrite — if the server
        // rotates the cookie (e.g. after a session refresh), we must pick up
        // the new value or every subsequent write will 401.
        for cookie in resp.cookies() {
            if cookie.name() == "tiled_csrf" {
                *self.inner.csrf_token.write().await = Some(cookie.value().to_string());
                return;
            }
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
        let providers: Vec<crate::auth::AuthProvider> = info
            .authentication
            .providers
            .iter()
            .filter_map(|v| crate::auth::AuthProvider::from_json(v, Some(&self.inner.api_uri)).ok())
            .collect();
        if providers.is_empty() {
            return Err(ClientError::AuthRequired(
                "server has no authentication providers".into(),
            ));
        }
        let tokens = crate::auth::prompt_for_credentials(&self.inner.http, &providers).await?;
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
            Some(crate::auth::token_directory_for_server(&self.inner.api_uri))
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
            TiledAuth::new(refresh_url, csrf, Some(dir), client_id)?
        } else {
            TiledAuth::in_memory(refresh_url, csrf, client_id)
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
        let csrf = self.csrf_token().await.unwrap_or_default();
        let dir = crate::auth::token_directory_for_server(&self.inner.api_uri);
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
        let auth = TiledAuth::new(refresh_url, csrf, Some(dir), client_id)?;
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
    pub async fn whoami(&self) -> Result<crate::auth::WhoAmI> {
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
        decode_response::<tiled_core::schemas::Response<crate::auth::WhoAmI>>(resp)
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
}
