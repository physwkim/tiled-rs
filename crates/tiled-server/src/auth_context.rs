//! Per-request auth context.
//!
//! The auth middleware decodes the incoming credential (single-user API
//! key, multi-user API key, or Bearer JWT) and inserts an [`AuthContext`]
//! into the request's extensions so route handlers can call
//! `auth.require(Scope::WriteMetadata)?` without re-walking the auth DB.

use std::sync::Arc;

use axum::extract::FromRequestParts;
use axum::http::request::Parts;

use tiled_auth::{Principal, Scope, ScopeSet};

use crate::error::ServerError;

#[derive(Debug, Clone)]
pub struct AuthContext {
    pub principal: Option<Arc<Principal>>,
    pub scopes: ScopeSet,
    pub kind: AuthKind,
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
        }
    }

    pub fn require(&self, scope: Scope) -> Result<(), ServerError> {
        if self.scopes.contains(scope) {
            Ok(())
        } else {
            Err(ServerError::Forbidden(format!(
                "missing scope: {}",
                scope.as_str()
            )))
        }
    }

    /// Apply per-node policy decision, returning a narrowed `AuthContext`.
    /// Used by handlers that resolved the target node and want the
    /// AccessPolicy (tiled#287) to weigh in. Falls through to `self` when
    /// no policy is wired.
    pub async fn narrow_for_node<'a>(
        &self,
        policy: Option<&dyn tiled_access::AccessPolicy>,
        ctx: tiled_access::NodeContext<'a>,
    ) -> AuthContext {
        let Some(policy) = policy else {
            return self.clone();
        };
        let decision = match self.principal.as_ref() {
            Some(p) => {
                policy
                    .principal_decision(p.as_ref(), &self.scopes, ctx)
                    .await
            }
            None => policy.anonymous_decision(ctx).await,
        };
        AuthContext {
            principal: self.principal.clone(),
            scopes: decision.scopes,
            kind: self.kind.clone(),
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
