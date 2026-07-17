//! Top-level entry points: `from_uri`, `from_context`.
//!
//! Mirrors `tiled/client/constructors.py`.

use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use serde::Deserialize;
use url::Url;

use crate::client::any_client::AnyClient;
use crate::client::base::Item;
use crate::client::context::{Context, ContextOptions};
use crate::client::error::{ClientError, Result};
use crate::client::utils::{decode_response, retry};

const PATH_SEGMENT: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'"')
    .add(b'<')
    .add(b'>')
    .add(b'#')
    .add(b'?')
    .add(b'`')
    .add(b'{')
    .add(b'}')
    .add(b'/')
    .add(b'%');

/// Connect to a Tiled server and return the client at the requested path.
///
/// Mirrors `tiled.client.from_uri`. The URI may include `?api_key=...` (which
/// is moved into a header) and a path past `/api/v1/` (which is treated as the
/// node to navigate to).
pub async fn from_uri(uri: &str) -> Result<AnyClient> {
    from_uri_with_options(uri, ContextOptions::default(), false).await
}

/// Like [`from_uri`] but with explicit options (api key, http client, ...).
pub async fn from_uri_with_options(
    uri: &str,
    options: ContextOptions,
    include_data_sources: bool,
) -> Result<AnyClient> {
    // Upstream default is remember_me=True (`constructors.py::from_uri`).
    let remember_me = options.remember_me.unwrap_or(true);
    let (ctx, parts) = Context::from_uri_with_options(uri, options)?;
    from_context(ctx, &parts, include_data_sources, remember_me).await
}

/// Non-interactive session resume during construction. Mirrors the auth branch
/// of upstream `from_context` (`constructors.py:150-165`):
///
/// - Already authenticated (api key or live tokens): nothing to do.
/// - Server requires auth but advertises no providers: only an API key can
///   satisfy it, so surface a clear error rather than a later opaque 401.
/// - Providers advertised and `remember_me` set: try cached tokens on disk via
///   [`Context::use_cached_tokens`] (keyed by the API URI's token directory). A
///   stale-but-refreshable access token is refreshed by the existing auth path
///   inside `whoami`; missing/invalid tokens fall through anonymously.
///
/// Interactive login (upstream `context.authenticate()`) is deliberately NOT
/// performed here — prompting from inside a library constructor is a UX policy
/// left to the CLI layer.
async fn maybe_resume_session(context: &Context, remember_me: bool) -> Result<()> {
    if context.authenticated().await {
        return Ok(());
    }
    let (auth_required, has_providers) = {
        let info = context.server_info().await?;
        (
            info.authentication.required,
            !info.authentication.providers.is_empty(),
        )
    };
    if auth_required && !has_providers {
        return Err(ClientError::AuthRequired(
            "server requires API key authentication; construct the client with an api_key".into(),
        ));
    }
    if has_providers && remember_me {
        // Best-effort: a missing/corrupt token cache must not brick
        // construction — fall through and connect anonymously.
        if let Err(e) = context.use_cached_tokens().await {
            tracing::debug!(
                target: "tiled.client",
                "cached-token session resume failed, continuing anonymously: {e}"
            );
        }
    }
    Ok(())
}

/// Connect using a pre-built `Context`. The `node_path_parts` walk past
/// `/api/v1/metadata/` to land at the requested node (empty = root).
///
/// `remember_me` gates non-interactive session resume (see
/// [`maybe_resume_session`]); the upstream default is `true`.
pub async fn from_context(
    context: Context,
    node_path_parts: &[String],
    include_data_sources: bool,
    remember_me: bool,
) -> Result<AnyClient> {
    maybe_resume_session(&context, remember_me).await?;
    let mut url = context.api_uri().clone();
    let encoded: Vec<String> = node_path_parts
        .iter()
        .map(|p| utf8_percent_encode(p, PATH_SEGMENT).to_string())
        .collect();
    let path_segment = format!("{}metadata/{}", url.path(), encoded.join("/"));
    url.set_path(&path_segment);
    if include_data_sources {
        url.query_pairs_mut()
            .append_pair("include_data_sources", "true");
    }
    let item = retry(|| async {
        let resp = context.get(&url).await?;
        let envelope: ResourceEnvelope = decode_response(resp).await?;
        envelope
            .data
            .ok_or_else(|| ClientError::Invalid("metadata response missing data".into()))
    })
    .await?;
    AnyClient::from_item(context, item, include_data_sources)
}

/// Build the URL for a metadata fetch — used by tests + advanced callers.
#[doc(hidden)]
pub fn metadata_url(api_uri: &Url, node_path_parts: &[String]) -> Result<Url> {
    let mut url = api_uri.clone();
    let path = format!("{}metadata/{}", url.path(), node_path_parts.join("/"));
    url.set_path(&path);
    Ok(url)
}

#[derive(Debug, Deserialize)]
struct ResourceEnvelope {
    data: Option<Item>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::context::ContextOptions;
    use axum::extract::State;
    use axum::routing::get;
    use axum::{Json, Router};
    use tokio::net::TcpListener;

    /// Minimal `About` payload for the `GET /api/v1/` discovery request.
    fn about(required: bool, providers: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "api_version": 0,
            "library_version": "test",
            "formats": {},
            "aliases": {},
            "queries": [],
            "authentication": {
                "required": required,
                "providers": providers,
                "links": {
                    "refresh_session": "/api/v1/auth/session/refresh",
                    "whoami": "/api/v1/auth/whoami",
                    "logout": null,
                },
            },
            "links": {},
            "meta": {},
        })
    }

    async fn about_handler(State(payload): State<serde_json::Value>) -> Json<serde_json::Value> {
        Json(payload)
    }

    /// Spawn a server that answers only the `About` discovery request — enough
    /// to exercise the resume decision without a full node response.
    async fn spawn_about(payload: serde_json::Value) -> String {
        let app = Router::new()
            .route("/api/v1/", get(about_handler))
            .with_state(payload);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        format!("http://{addr}")
    }

    // Regression: an api-key context is already authenticated, so resume is a
    // no-op — it must not touch the network or clear the key.
    #[tokio::test]
    async fn resume_noop_when_api_key_present() {
        let base = spawn_about(about(true, serde_json::json!([]))).await;
        let (ctx, _) =
            Context::from_uri_with_options(&base, ContextOptions::default().api_key("secret"))
                .unwrap();
        maybe_resume_session(&ctx, true).await.unwrap();
        assert!(ctx.authenticated().await);
        assert_eq!(ctx.api_key().await.as_deref(), Some("secret"));
    }

    // Regression: a no-auth server (required=false, no providers) resumes to a
    // no-op and stays anonymous — construction is unchanged.
    #[tokio::test]
    async fn resume_noop_on_no_auth_server() {
        let base = spawn_about(about(false, serde_json::json!([]))).await;
        let (ctx, _) = Context::from_uri_with_options(&base, ContextOptions::default()).unwrap();
        maybe_resume_session(&ctx, true).await.unwrap();
        assert!(!ctx.authenticated().await);
    }

    // Boundary: a server that requires auth but advertises no providers can
    // only be satisfied by an API key — surface a clear error.
    #[tokio::test]
    async fn resume_errors_when_required_but_no_providers() {
        let base = spawn_about(about(true, serde_json::json!([]))).await;
        let (ctx, _) = Context::from_uri_with_options(&base, ContextOptions::default()).unwrap();
        let err = maybe_resume_session(&ctx, true).await.unwrap_err();
        assert!(
            matches!(err, ClientError::AuthRequired(_)),
            "expected AuthRequired, got {err:?}"
        );
    }

    // Boundary: with providers advertised but no cached tokens on disk, resume
    // is attempted and falls through anonymously (no hang, no error). The
    // random loopback port makes the per-server token directory non-existent.
    #[tokio::test]
    async fn resume_attempts_but_falls_through_without_cached_tokens() {
        let providers = serde_json::json!([{
            "provider": "toy",
            "mode": "password",
            "links": {},
        }]);
        let base = spawn_about(about(false, providers)).await;
        let (ctx, _) = Context::from_uri_with_options(&base, ContextOptions::default()).unwrap();
        maybe_resume_session(&ctx, true).await.unwrap();
        assert!(!ctx.authenticated().await);
    }

    // Boundary: remember_me=false suppresses the cached-token resume entirely,
    // even when providers are advertised.
    #[tokio::test]
    async fn resume_skipped_when_remember_me_false() {
        let providers = serde_json::json!([{
            "provider": "toy",
            "mode": "password",
            "links": {},
        }]);
        let base = spawn_about(about(false, providers)).await;
        let (ctx, _) = Context::from_uri_with_options(&base, ContextOptions::default()).unwrap();
        maybe_resume_session(&ctx, false).await.unwrap();
        assert!(!ctx.authenticated().await);
    }
}
