//! OAuth2 authentication: token storage, refresh, password / device-code grants.
//!
//! Mirrors `tiled/client/auth.py` (`TiledAuth`, `build_refresh_request`,
//! `CannotRefreshAuthentication`) plus the auth-related helpers in
//! `tiled/client/context.py` (`password_grant`, `device_code_grant`,
//! `prompt_for_credentials`, `whoami`, `logout`).
//!
//! The Python `TiledAuth` is a generator-based `httpx.Auth` that yields
//! requests and intercepts 401s. reqwest has no equivalent middleware shape;
//! we instead expose the components and let the `Context` orchestrate the
//! "send → 401? → refresh → retry once" loop in one place.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use url::Url;

use crate::error::{ClientError, Result};
use crate::utils::handle_error;

/// Three tokens returned by every OIDC grant flow.
///
/// `Debug` deliberately redacts the token values — formatting one with `{:?}`
/// (e.g. inside a `tracing` event) prints `<set>` rather than the raw bearer
/// token, so casual debug logging can't leak the credential.
#[derive(Clone, Deserialize, Serialize)]
pub struct Tokens {
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default)]
    pub id_token: Option<String>,
}

impl std::fmt::Debug for Tokens {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Tokens")
            .field("access_token", &"<set>")
            .field("refresh_token", &"<set>")
            .field("id_token", &self.id_token.as_ref().map(|_| "<set>"))
            .finish()
    }
}

/// On-disk + in-memory token store, keyed by token name (`access_token`,
/// `refresh_token`, `id_token`). Mirrors `TiledAuth.sync_get_token` /
/// `sync_set_token` / `sync_clear_token`.
///
/// Disk layout: one file per token under
/// `<token_directory>/<token_name>`. File mode is 0o600.
pub struct TokenStore {
    /// Directory under which tokens are persisted, or `None` for in-memory.
    dir: Option<PathBuf>,
    /// In-memory cache. Updated on `set_token` and on disk reload.
    cache: Mutex<HashMap<String, String>>,
}

impl std::fmt::Debug for TokenStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Don't print the cache contents — they are token values.
        f.debug_struct("TokenStore")
            .field("dir", &self.dir)
            .field("cache", &"<redacted>")
            .finish()
    }
}

impl TokenStore {
    pub fn new(dir: Option<PathBuf>) -> Result<Self> {
        if let Some(d) = dir.as_ref() {
            std::fs::create_dir_all(d).map_err(|e| {
                ClientError::Invalid(format!("token directory {}: {e}", d.display()))
            })?;
            // Mirror `_check_writable_token_directory`.
            let meta = std::fs::metadata(d)
                .map_err(|e| ClientError::Invalid(format!("token directory metadata: {e}")))?;
            if meta.permissions().readonly() {
                return Err(ClientError::Invalid(format!(
                    "token directory {} is not writable",
                    d.display()
                )));
            }
        }
        Ok(Self {
            dir,
            cache: Mutex::new(HashMap::new()),
        })
    }

    pub fn in_memory() -> Self {
        Self {
            dir: None,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, key: &str, reload_from_disk: bool) -> Result<Option<String>> {
        if !reload_from_disk {
            let cache = self.cache.lock().await;
            if let Some(v) = cache.get(key) {
                return Ok(Some(v.clone()));
            }
        }
        let Some(dir) = &self.dir else {
            // No disk backing — return whatever's in memory.
            let cache = self.cache.lock().await;
            return Ok(cache.get(key).cloned());
        };
        let path = dir.join(key);
        match tokio::fs::read_to_string(&path).await {
            Ok(value) => {
                self.cache.lock().await.insert(key.into(), value.clone());
                Ok(Some(value))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // Disk doesn't have it — fall back to memory.
                let cache = self.cache.lock().await;
                Ok(cache.get(key).cloned())
            }
            Err(e) => Err(ClientError::Invalid(format!(
                "read token {}: {e}",
                path.display()
            ))),
        }
    }

    pub async fn set(&self, key: &str, value: &str) -> Result<()> {
        if let Some(dir) = &self.dir {
            // Atomic write: tempfile (uniquified by PID + monotonic counter
            // so concurrent set("access_token", ...) calls don't collide on
            // the same `.access_token.tmp`), chmod 0o600, then rename.
            // Crash mid-write leaves the previous value intact.
            use std::sync::atomic::{AtomicU64, Ordering};
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let nonce = COUNTER.fetch_add(1, Ordering::Relaxed);
            let pid = std::process::id();
            let final_path = dir.join(key);
            let tmp_path = dir.join(format!(".{key}.{pid}.{nonce}.tmp"));
            tokio::fs::write(&tmp_path, value).await.map_err(|e| {
                ClientError::Invalid(format!("write token tmpfile {}: {e}", tmp_path.display()))
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mut perms = tokio::fs::metadata(&tmp_path)
                    .await
                    .map_err(|e| ClientError::Invalid(format!("stat token tmp: {e}")))?
                    .permissions();
                perms.set_mode(0o600);
                tokio::fs::set_permissions(&tmp_path, perms)
                    .await
                    .map_err(|e| ClientError::Invalid(format!("chmod token tmp: {e}")))?;
            }
            if let Err(e) = tokio::fs::rename(&tmp_path, &final_path).await {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                return Err(ClientError::Invalid(format!(
                    "rename {} → {}: {e}",
                    tmp_path.display(),
                    final_path.display()
                )));
            }
        }
        self.cache.lock().await.insert(key.into(), value.into());
        Ok(())
    }

    pub async fn clear(&self, key: &str) -> Result<()> {
        if let Some(dir) = &self.dir {
            let path = dir.join(key);
            match tokio::fs::remove_file(&path).await {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => {
                    return Err(ClientError::Invalid(format!(
                        "remove token {}: {e}",
                        path.display()
                    )));
                }
            }
        }
        self.cache.lock().await.remove(key);
        Ok(())
    }

    pub async fn save_tokens(&self, tokens: &Tokens) -> Result<()> {
        // Save the longer-lived refresh_token first so that an interrupted
        // write (disk full, crash) leaves us able to refresh against the
        // session — losing only the short-lived access_token, which a 401
        // recovery cycle will re-fetch.
        self.set("refresh_token", &tokens.refresh_token).await?;
        self.set("access_token", &tokens.access_token).await?;
        if let Some(id) = &tokens.id_token {
            self.set("id_token", id).await?;
        }
        Ok(())
    }
}

/// OAuth2 / OIDC authentication state for a `Context`.
///
/// `Arc`-wrapped so it can be cheaply shared between the context and any
/// request retry loop.
#[derive(Clone)]
pub struct TiledAuth {
    pub(crate) inner: Arc<TiledAuthInner>,
}

impl std::fmt::Debug for TiledAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiledAuth")
            .field("refresh_url", &self.inner.refresh_url)
            .field("client_id", &self.inner.client_id)
            .field("csrf_token", &"<redacted>")
            .field("tokens", &"<redacted>")
            .finish()
    }
}

pub struct TiledAuthInner {
    pub(crate) refresh_url: Url,
    pub(crate) csrf_token: String,
    pub(crate) client_id: Option<String>,
    pub(crate) tokens: TokenStore,
}

impl TiledAuth {
    pub fn new(
        refresh_url: Url,
        csrf_token: impl Into<String>,
        token_directory: Option<PathBuf>,
        client_id: Option<String>,
    ) -> Result<Self> {
        let tokens = TokenStore::new(token_directory)?;
        Ok(Self {
            inner: Arc::new(TiledAuthInner {
                refresh_url,
                csrf_token: csrf_token.into(),
                client_id,
                tokens,
            }),
        })
    }

    pub fn in_memory(
        refresh_url: Url,
        csrf_token: impl Into<String>,
        client_id: Option<String>,
    ) -> Self {
        Self {
            inner: Arc::new(TiledAuthInner {
                refresh_url,
                csrf_token: csrf_token.into(),
                client_id,
                tokens: TokenStore::in_memory(),
            }),
        }
    }

    pub fn tokens(&self) -> &TokenStore {
        &self.inner.tokens
    }

    pub async fn save_tokens(&self, tokens: &Tokens) -> Result<()> {
        self.inner.tokens.save_tokens(tokens).await
    }

    /// Current `Authorization: Bearer <access_token>` value, if any.
    pub async fn auth_header(&self) -> Option<String> {
        self.inner
            .tokens
            .get("access_token", false)
            .await
            .ok()
            .flatten()
            .map(|t| format!("Bearer {t}"))
    }

    /// Full refresh flow: exchange refresh_token for a new access_token.
    /// Mirrors `force_auth_refresh`. On 401 the refresh_token is cleared and
    /// `CannotRefresh` is returned.
    pub async fn refresh(&self, http: &Client) -> Result<Tokens> {
        let refresh_token = self
            .inner
            .tokens
            .get("refresh_token", true)
            .await?
            .ok_or_else(|| {
                ClientError::AuthRequired("no refresh_token in cache; please log in again".into())
            })?;

        let resp = build_refresh_request(
            http,
            &self.inner.refresh_url,
            &refresh_token,
            &self.inner.csrf_token,
            self.inner.client_id.as_deref(),
        )
        .send()
        .await?;
        if resp.status().as_u16() == 401 {
            self.inner.tokens.clear("refresh_token").await?;
            return Err(ClientError::AuthRequired(
                "server rejected refresh_token; please log in again".into(),
            ));
        }
        let resp = handle_error(resp).await?;
        let tokens: Tokens = resp.json().await?;
        self.save_tokens(&tokens).await?;
        Ok(tokens)
    }
}

/// Build (but do not send) a token-refresh request.
///
/// OIDC client_id mode uses form-urlencoded; legacy Tiled native mode uses a
/// JSON body + `x-csrf` header (double-submit cookie).
pub fn build_refresh_request(
    http: &Client,
    refresh_url: &Url,
    refresh_token: &str,
    csrf_token: &str,
    client_id: Option<&str>,
) -> reqwest::RequestBuilder {
    if let Some(cid) = client_id {
        let form = [
            ("client_id", cid),
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
        ];
        http.post(refresh_url.as_str())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&form)
    } else {
        http.post(refresh_url.as_str())
            .json(&serde_json::json!({"refresh_token": refresh_token}))
            .header("x-csrf", csrf_token)
    }
}

/// Resource-owner password grant.
pub async fn password_grant(
    http: &Client,
    auth_endpoint: &Url,
    _provider: &str,
    username: &str,
    password: &str,
) -> Result<Tokens> {
    let form = [
        ("grant_type", "password"),
        ("username", username),
        ("password", password),
    ];
    let resp = http.post(auth_endpoint.as_str()).form(&form).send().await?;
    let resp = handle_error(resp).await?;
    Ok(resp.json().await?)
}

/// OAuth2 device-code grant. Polls `token_endpoint` (or
/// `verification_uri`) until the user completes login or `expires_in` elapses.
///
/// Opens the verification URI in the system browser if available.
pub async fn device_code_grant(
    http: &Client,
    auth_endpoint: &Url,
    client_id: Option<&str>,
    token_endpoint: Option<&Url>,
    scopes: &str,
) -> Result<Tokens> {
    let oauth2_spec = client_id.is_some() && token_endpoint.is_some();

    let verification_resp = if oauth2_spec {
        let cid = client_id.expect("guarded above");
        let form = [("client_id", cid), ("scope", scopes)];
        let r = http.post(auth_endpoint.as_str()).form(&form).send().await?;
        handle_error(r).await?
    } else {
        let r = http.post(auth_endpoint.as_str()).send().await?;
        handle_error(r).await?
    };

    let verification: serde_json::Value = verification_resp.json().await?;
    let uri_field = if oauth2_spec {
        "verification_uri_complete"
    } else {
        "authorization_uri"
    };
    let authorization_uri = verification
        .get(uri_field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| ClientError::Invalid(format!("device-code response missing '{uri_field}'")))?
        .to_string();
    let user_code = verification
        .get("user_code")
        .and_then(|v| v.as_str())
        .unwrap_or("(none)");
    let expires_in = verification
        .get("expires_in")
        .and_then(|v| v.as_u64())
        .unwrap_or(300);
    let interval = verification
        .get("interval")
        .and_then(|v| v.as_u64())
        .unwrap_or(5);
    let device_code = verification
        .get("device_code")
        .and_then(|v| v.as_str())
        .ok_or_else(|| ClientError::Invalid("device-code response missing 'device_code'".into()))?
        .to_string();
    let polling_uri = if oauth2_spec {
        token_endpoint.expect("guarded").as_str().to_string()
    } else {
        verification
            .get("verification_uri")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                ClientError::Invalid("device-code response missing 'verification_uri'".into())
            })?
            .to_string()
    };

    println!(
        "\nYou have {} minutes to visit this URL\n\n{}\n\nand enter the code:\n\n{}\n",
        expires_in / 60,
        authorization_uri,
        user_code
    );
    let _ = webbrowser::open(&authorization_uri);

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(expires_in);
    print!("Waiting...");
    use std::io::Write;
    let _ = std::io::stdout().flush();

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(interval)).await;
        if std::time::Instant::now() > deadline {
            return Err(ClientError::AuthRequired(
                "device-code grant: deadline expired".into(),
            ));
        }
        let resp = if oauth2_spec {
            let form = [
                ("device_code", device_code.as_str()),
                ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                ("client_id", client_id.expect("guarded")),
            ];
            http.post(&polling_uri).form(&form).send().await?
        } else {
            http.post(&polling_uri)
                .json(&serde_json::json!({
                    "device_code": &device_code,
                    "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
                }))
                .send()
                .await?
        };
        let status = resp.status();
        if status == 400 {
            // Could be authorization_pending — keep polling.
            let body: serde_json::Value = resp.json().await.unwrap_or(serde_json::Value::Null);
            let error_field = if oauth2_spec {
                body.get("error").and_then(|v| v.as_str())
            } else {
                body.pointer("/detail/error").and_then(|v| v.as_str())
            };
            if error_field == Some("authorization_pending") {
                print!(".");
                let _ = std::io::stdout().flush();
                continue;
            }
            return Err(ClientError::AuthRequired(format!(
                "device-code 400: {body}"
            )));
        }
        let resp = handle_error(resp).await?;
        let tokens: Tokens = resp.json().await?;
        println!();
        return Ok(tokens);
    }
}

/// Default token cache directory: `<XDG_CACHE_HOME>/tiled/tokens/`.
pub fn default_token_cache_dir() -> PathBuf {
    if let Ok(custom) = std::env::var("TILED_CACHE_DIR") {
        return PathBuf::from(custom).join("tokens");
    }
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("tiled")
        .join("tokens")
}

/// Per-server token directory. Mirrors `Context._token_directory`.
pub fn token_directory_for_server(api_uri: &Url) -> PathBuf {
    let host = utf8_percent_encode(api_uri.as_str(), NON_ALPHANUMERIC).to_string();
    default_token_cache_dir().join(host)
}

/// Read a username from stdin.
pub fn prompt_username() -> Result<String> {
    use std::io::{BufRead, Write};
    print!("Username: ");
    std::io::stdout()
        .flush()
        .map_err(|e| ClientError::Invalid(format!("stdout flush: {e}")))?;
    let stdin = std::io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| ClientError::Invalid(format!("read username: {e}")))?;
    Ok(line.trim().to_string())
}

/// Read a password without echoing.
pub fn prompt_password() -> Result<String> {
    rpassword::prompt_password("Password: ")
        .map_err(|e| ClientError::Invalid(format!("password prompt: {e}")))
}

/// Convenience flag: are we running in a context that can prompt the user?
pub fn can_prompt() -> bool {
    if std::env::var("TILED_FORCE_PROMPT").as_deref() == Ok("1") {
        return true;
    }
    use std::io::IsTerminal;
    std::io::stdin().is_terminal()
}

/// Provider mode (`internal` = password grant, `external` = device code).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderMode {
    Internal,
    External,
}

/// One authentication provider, parsed from `About.authentication.providers`.
#[derive(Debug, Clone)]
pub struct AuthProvider {
    pub provider: String,
    pub mode: ProviderMode,
    pub auth_endpoint: Url,
    pub client_id: Option<String>,
    pub token_endpoint: Option<Url>,
    pub confirmation_message: Option<String>,
}

impl AuthProvider {
    /// Decode from the loose `serde_json::Value` shape that
    /// `tiled-core`'s `About.authentication.providers` uses today.
    ///
    /// Pass `base` so relative endpoint URLs (`/auth/provider/foo/login`)
    /// resolve correctly. If `base` is `None` we still parse absolute URLs.
    pub fn from_json(value: &serde_json::Value, base: Option<&Url>) -> Result<Self> {
        let provider = value
            .get("provider")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ClientError::Invalid("provider missing 'provider' name".into()))?
            .to_string();
        let mode_str = value
            .get("mode")
            .and_then(|v| v.as_str())
            .unwrap_or("internal");
        let mode = match mode_str {
            "internal" | "password" => ProviderMode::Internal,
            "external" => ProviderMode::External,
            other => {
                return Err(ClientError::Invalid(format!(
                    "unknown provider mode '{other}'"
                )));
            }
        };
        let links = value
            .get("links")
            .ok_or_else(|| ClientError::Invalid("provider missing 'links'".into()))?;
        let parse_link = |s: &str| -> Result<Url> {
            if let Ok(u) = Url::parse(s) {
                return Ok(u);
            }
            match base {
                Some(b) => b.join(s.trim_start_matches('/')).map_err(ClientError::from),
                None => Err(ClientError::Invalid(format!(
                    "relative provider link '{s}' but no base URL given"
                ))),
            }
        };
        let auth_endpoint = links
            .get("auth_endpoint")
            .and_then(|v| v.as_str())
            .ok_or_else(|| ClientError::Invalid("provider missing 'links.auth_endpoint'".into()))?;
        let auth_endpoint = parse_link(auth_endpoint)?;
        let client_id = links
            .get("client_id")
            .and_then(|v| v.as_str())
            .map(String::from);
        let token_endpoint = match links.get("token_endpoint").and_then(|v| v.as_str()) {
            Some(s) => Some(parse_link(s)?),
            None => None,
        };
        let confirmation_message = value
            .get("confirmation_message")
            .and_then(|v| v.as_str())
            .map(String::from);
        Ok(Self {
            provider,
            mode,
            auth_endpoint,
            client_id,
            token_endpoint,
            confirmation_message,
        })
    }
}

/// Interactive: walk the user through choosing a provider and entering creds.
/// Mirrors `prompt_for_credentials`.
pub async fn prompt_for_credentials(http: &Client, providers: &[AuthProvider]) -> Result<Tokens> {
    if providers.is_empty() {
        return Err(ClientError::AuthRequired(
            "server has no authentication providers configured".into(),
        ));
    }
    let provider = if providers.len() == 1 {
        &providers[0]
    } else {
        let idx = pick_provider(providers)?;
        &providers[idx]
    };

    match provider.mode {
        ProviderMode::Internal => {
            let username = prompt_username()?;
            for _ in 0..3 {
                let password = prompt_password()?;
                if password.is_empty() {
                    return Err(ClientError::AuthRequired("empty password".into()));
                }
                match password_grant(
                    http,
                    &provider.auth_endpoint,
                    &provider.provider,
                    &username,
                    &password,
                )
                .await
                {
                    Ok(tokens) => return Ok(tokens),
                    Err(ClientError::AuthRequired(_)) => {
                        eprintln!("Username or password not recognized. Retry.");
                    }
                    Err(e) => return Err(e),
                }
            }
            Err(ClientError::AuthRequired(
                "password rejected after 3 attempts".into(),
            ))
        }
        ProviderMode::External => {
            device_code_grant(
                http,
                &provider.auth_endpoint,
                provider.client_id.as_deref(),
                provider.token_endpoint.as_ref(),
                "openid offline_access",
            )
            .await
        }
    }
}

fn pick_provider(providers: &[AuthProvider]) -> Result<usize> {
    use std::io::{BufRead, Write};
    println!("Authentication providers:");
    for (i, p) in providers.iter().enumerate() {
        println!("{} - {}", i + 1, p.provider);
    }
    print!("Choose a provider (1..{}): ", providers.len());
    std::io::stdout()
        .flush()
        .map_err(|e| ClientError::Invalid(format!("stdout flush: {e}")))?;
    let stdin = std::io::stdin();
    let mut line = String::new();
    stdin
        .lock()
        .read_line(&mut line)
        .map_err(|e| ClientError::Invalid(format!("read choice: {e}")))?;
    let choice: usize = line
        .trim()
        .parse()
        .map_err(|_| ClientError::Invalid("provider choice must be a number".into()))?;
    if choice < 1 || choice > providers.len() {
        return Err(ClientError::Invalid(format!(
            "choice must be 1..{}",
            providers.len()
        )));
    }
    Ok(choice - 1)
}

/// User identity returned by `whoami`.
#[derive(Debug, Clone, Deserialize)]
pub struct WhoAmI {
    #[serde(default, rename = "type")]
    pub kind: Option<String>,
    #[serde(default)]
    pub uuid: Option<String>,
    #[serde(default)]
    pub identities: Vec<WhoAmIIdentity>,
    #[serde(flatten)]
    pub extra: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WhoAmIIdentity {
    pub id: String,
    #[serde(default)]
    pub provider: Option<String>,
}

/// Path helper used by the `Path` extension trait below in tests.
#[doc(hidden)]
pub fn _ensure_path_exists(p: &Path) -> bool {
    p.exists()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn token_store_roundtrip_in_memory() {
        let s = TokenStore::in_memory();
        s.set("access_token", "abc").await.unwrap();
        assert_eq!(
            s.get("access_token", false).await.unwrap(),
            Some("abc".into())
        );
        s.clear("access_token").await.unwrap();
        assert_eq!(s.get("access_token", false).await.unwrap(), None);
    }

    #[tokio::test]
    async fn token_store_roundtrip_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let s = TokenStore::new(Some(dir.path().to_path_buf())).unwrap();
        s.set("refresh_token", "xyz").await.unwrap();
        let path = dir.path().join("refresh_token");
        assert!(path.exists());
        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body, "xyz");

        // Reload via fresh instance.
        let s2 = TokenStore::new(Some(dir.path().to_path_buf())).unwrap();
        assert_eq!(
            s2.get("refresh_token", false).await.unwrap(),
            Some("xyz".into())
        );
    }

    #[test]
    fn token_directory_per_server() {
        let url = Url::parse("http://example.com:8000/api/v1/").unwrap();
        let p = token_directory_for_server(&url);
        assert!(
            p.to_string_lossy().contains("example"),
            "path should contain encoded host: {p:?}"
        );
    }
}
