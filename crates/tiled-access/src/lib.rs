//! Access policy hooks.
//!
//! Mirrors `tiled.access_policies`. The server queries an [`AccessPolicy`]
//! impl per request to decide:
//! 1. Which scopes the principal actually has on this specific node
//!    (in addition to the scopes baked into their JWT).
//! 2. Whether a search query should be filtered down further (e.g. tag-
//!    based row-level access).
//!
//! The trait is intentionally narrow so a tag-based, role-based, or
//! external-PDP policy can all plug in without touching the server.

use async_trait::async_trait;

pub use tiled_auth::{Principal, Scope, ScopeSet};

/// Lightweight description of the node the policy is evaluating.
#[derive(Debug, Clone)]
pub struct NodeContext<'a> {
    pub path: &'a [String],
    pub structure_family: &'a str,
    /// JSON metadata of the node — useful for tag-based decisions.
    pub metadata: &'a serde_json::Value,
    /// `access_blob` from the catalog row (e.g. `{"tags": ["public"]}`).
    pub access_blob: &'a serde_json::Value,
}

/// What the policy decides for a given (principal, node) pair.
#[derive(Debug, Clone)]
pub struct Decision {
    /// Effective scopes the principal has on this node. Always a
    /// subset of the principal's session scopes.
    pub scopes: ScopeSet,
    /// Optional filter to AND into a search query so listings only
    /// return rows the principal can actually see. `None` = no extra
    /// filter.
    pub search_filter: Option<serde_json::Value>,
}

#[async_trait]
pub trait AccessPolicy: Send + Sync {
    /// Default scopes for unauthenticated callers, by node. The server
    /// uses this when the request arrives without any credential.
    async fn anonymous_decision(&self, ctx: NodeContext<'_>) -> Decision;

    /// Decision for an authenticated principal. The server passes the
    /// JWT/session scopes via `session_scopes`; the policy may narrow
    /// them but must NOT widen them.
    async fn principal_decision(
        &self,
        principal: &Principal,
        session_scopes: &ScopeSet,
        ctx: NodeContext<'_>,
    ) -> Decision;
}

/// Built-in: trust the session — anonymous gets read-only metadata,
/// authenticated principals keep their session scopes verbatim. Useful
/// when scope assignment lives entirely in the auth DB.
pub struct PassthroughPolicy;

#[async_trait]
impl AccessPolicy for PassthroughPolicy {
    async fn anonymous_decision(&self, _ctx: NodeContext<'_>) -> Decision {
        Decision {
            scopes: ScopeSet::default(),
            search_filter: None,
        }
    }

    async fn principal_decision(
        &self,
        _principal: &Principal,
        session_scopes: &ScopeSet,
        _ctx: NodeContext<'_>,
    ) -> Decision {
        Decision {
            scopes: session_scopes.clone(),
            search_filter: None,
        }
    }
}

/// Built-in: tag-based policy.
///
/// Each principal owns a set of tags (typically supplied through the
/// constructor or an out-of-band sync). A node is visible iff its
/// `access_blob.tags` array intersects the principal's tag set, OR the
/// node has no tags (treated as public).
pub struct TagBasedPolicy {
    /// Map from principal UUID → granted tags.
    pub principal_tags: std::collections::HashMap<String, Vec<String>>,
    /// Default scopes when a tagged node matches.
    pub default_scopes: ScopeSet,
}

impl TagBasedPolicy {
    pub fn new(default_scopes: ScopeSet) -> Self {
        Self {
            principal_tags: std::collections::HashMap::new(),
            default_scopes,
        }
    }

    pub fn grant(&mut self, principal_uuid: &str, tag: &str) {
        self.principal_tags
            .entry(principal_uuid.to_string())
            .or_default()
            .push(tag.to_string());
    }

    fn node_tags(ctx: &NodeContext<'_>) -> Vec<String> {
        ctx.access_blob
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[async_trait]
impl AccessPolicy for TagBasedPolicy {
    async fn anonymous_decision(&self, ctx: NodeContext<'_>) -> Decision {
        // Anonymous sees only "public" nodes (no tags).
        if Self::node_tags(&ctx).is_empty() {
            Decision {
                scopes: ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData]),
                search_filter: None,
            }
        } else {
            Decision {
                scopes: ScopeSet::default(),
                search_filter: None,
            }
        }
    }

    async fn principal_decision(
        &self,
        principal: &Principal,
        session_scopes: &ScopeSet,
        ctx: NodeContext<'_>,
    ) -> Decision {
        let node_tags = Self::node_tags(&ctx);
        let granted = self
            .principal_tags
            .get(&principal.uuid)
            .cloned()
            .unwrap_or_default();
        let visible = node_tags.is_empty() || node_tags.iter().any(|t| granted.contains(t));
        if visible {
            Decision {
                scopes: session_scopes.intersect(&self.default_scopes),
                search_filter: None,
            }
        } else {
            Decision {
                scopes: ScopeSet::default(),
                search_filter: None,
            }
        }
    }
}
