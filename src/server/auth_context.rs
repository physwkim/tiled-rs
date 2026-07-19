//! Per-request auth context.
//!
//! The auth middleware decodes the incoming credential (single-user API
//! key, multi-user API key, or Bearer JWT) and inserts an [`AuthContext`]
//! into the request's extensions so route handlers can call
//! `auth.require(Scope::WriteMetadata)?` without re-walking the auth DB.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use crate::auth::{Principal, Scope, ScopeSet};

use crate::server::error::ServerError;

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub principal: Option<Arc<Principal>>,
    pub scopes: ScopeSet,
    pub kind: AuthKind,
    /// API-key tag restriction. When `Some`, the key's `access_tags` were
    /// non-empty; the effective tag grant is the intersection of the
    /// principal's DB tags and this set (authn_access_tags narrowing).
    /// `None` means the credential carries no tag restriction (session,
    /// single-user key, proxied auth, or an API key with empty access_tags).
    pub authn_access_tags: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthKind {
    Anonymous,
    SingleUserKey,
    ApiKey,
    Session,
    Proxied,
}

impl AuthContext {
    pub fn anonymous() -> Self {
        Self {
            principal: None,
            scopes: ScopeSet::default(),
            kind: AuthKind::Anonymous,
            authn_access_tags: None,
        }
    }

    /// GLOBAL route-scope gate — the analog of upstream's
    /// `Security(check_scopes, scopes=[scope])` (authentication.py:544-570).
    /// The un-narrowed credential must carry `scope`; if not, this yields a
    /// **401** with `WWW-Authenticate: Bearer scope="<scope>"`, exactly as
    /// upstream's `check_scopes` does for the route-level gate. Distinct from
    /// [`Self::require_on_node`] (403), the per-node gate.
    pub fn require(&self, scope: Scope) -> Result<(), ServerError> {
        if self.scopes.contains(scope) {
            Ok(())
        } else {
            Err(ServerError::InsufficientScope {
                detail: format!("Not enough permissions. Requires scope {}.", scope.as_str()),
                scopes: scope.as_str().to_string(),
            })
        }
    }

    /// PER-NODE scope gate — the analog of upstream `get_entry`'s
    /// `allowed_scopes` check on a *visible* node (dependencies.py:117-133):
    /// the caller can see this node (`read:metadata` already passed) but the
    /// access policy narrowed away `scope` on it. Yields **403** with
    /// upstream's detail wording. Call this on the post-`narrow_for_node`
    /// context; use [`Self::require`] (401) for the route-level global gate.
    pub fn require_on_node(&self, scope: Scope) -> Result<(), ServerError> {
        if self.scopes.contains(scope) {
            Ok(())
        } else {
            let had: Vec<&str> = self.scopes.iter().map(|s| s.as_str()).collect();
            Err(ServerError::Forbidden(format!(
                "Not enough permissions to perform this action on this node. \
                 Requires scopes {}. Principal had scopes {:?} on this node.",
                scope.as_str(),
                had
            )))
        }
    }

    /// Apply per-node policy decision, returning a narrowed `AuthContext`.
    /// Used by handlers that resolved the target node and want the
    /// AccessPolicy (tiled#287) to weigh in. Falls through to `self` when
    /// no policy is wired.
    pub async fn narrow_for_node<'a>(
        &self,
        policy: Option<&dyn crate::access::AccessPolicy>,
        ctx: crate::access::NodeContext<'a>,
    ) -> AuthContext {
        let Some(policy) = policy else {
            return self.clone();
        };
        let decision = match self.principal.as_ref() {
            Some(p) => {
                policy
                    .principal_decision(
                        p.as_ref(),
                        &self.scopes,
                        self.authn_access_tags.as_deref(),
                        ctx,
                    )
                    .await
            }
            None => policy.anonymous_decision(ctx).await,
        };
        AuthContext {
            principal: self.principal.clone(),
            scopes: decision.scopes,
            kind: self.kind.clone(),
            authn_access_tags: self.authn_access_tags.clone(),
        }
    }
}

impl<S: Send + Sync> FromRequestParts<S> for AuthContext {
    type Rejection = std::convert::Infallible;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let ctx = parts
            .extensions
            .get::<AuthContext>()
            .cloned()
            .unwrap_or_else(AuthContext::anonymous);
        std::future::ready(Ok(ctx))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{StatusCode, header};
    use axum::response::IntoResponse;

    fn ctx_with(scopes: ScopeSet) -> AuthContext {
        AuthContext {
            principal: None,
            scopes,
            kind: AuthKind::Anonymous,
            authn_access_tags: None,
        }
    }

    // F1: the GLOBAL route-scope gate (upstream `check_scopes`) must be 401,
    // NOT 403, and must carry a `WWW-Authenticate: Bearer scope="..."` header.
    #[test]
    fn require_missing_scope_is_401_with_www_authenticate() {
        let ctx = ctx_with(ScopeSet::read_only()); // read:metadata + read:data only
        let err = ctx.require(Scope::WriteMetadata).unwrap_err();
        assert!(
            matches!(err, ServerError::InsufficientScope { .. }),
            "expected InsufficientScope, got {err:?}"
        );
        let resp = err.into_response();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "global route-scope gate must be 401 (upstream check_scopes)"
        );
        let wa = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .expect("401 must carry WWW-Authenticate")
            .to_str()
            .unwrap();
        assert!(
            wa.contains("Bearer") && wa.contains("write:metadata"),
            "WWW-Authenticate should name the required scope, got {wa}"
        );
    }

    #[test]
    fn require_present_scope_ok() {
        let ctx = ctx_with(ScopeSet::read_only());
        assert!(ctx.require(Scope::ReadMetadata).is_ok());
    }

    // F1: the PER-NODE gate (upstream `get_entry` on a visible node) must
    // STAY 403 — a caller that can see the node but lacks the operation scope.
    #[test]
    fn require_on_node_missing_scope_is_403() {
        let ctx = ctx_with(ScopeSet::read_only());
        let err = ctx.require_on_node(Scope::WriteMetadata).unwrap_err();
        assert!(
            matches!(err, ServerError::Forbidden(_)),
            "expected Forbidden, got {err:?}"
        );
        let resp = err.into_response();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "per-node scope denial must stay 403 (upstream get_entry)"
        );
    }

    #[test]
    fn require_on_node_present_scope_ok() {
        let ctx = ctx_with(ScopeSet::read_only());
        assert!(ctx.require_on_node(Scope::ReadData).is_ok());
    }
}
