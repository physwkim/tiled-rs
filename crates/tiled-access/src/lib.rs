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
use tiled_core::queries::AccessBlobFilter;

pub use tiled_auth::{Principal, Scope, ScopeSet};

/// The built-in always-public tag. A node tagged with this exact string is
/// world-readable, mirroring Python's `public_tag` (access_policies.py:90,
/// `is_tag_public`). Both the per-node decision and the list filter honour it,
/// so a `"public"`-tagged node is consistently readable AND listable by
/// everyone on read-scoped operations. (Python casefolds the comparison; this
/// built-in uses an exact match so the SQL tag IN-list and the in-memory check
/// stay consistent — the lowercase spelling is the convention.)
const PUBLIC_TAG: &str = "public";

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

    /// Filter to AND into listing/search queries so a principal only sees
    /// nodes they are permitted to access. `None` means no extra filter
    /// (all nodes visible — passthrough / ALL_ACCESS behaviour).
    ///
    /// Called once per search/list request, NOT per node.  The returned
    /// filter is pushed down to SQL via [`AccessBlobFilter`] so that
    /// unpermitted rows never leave the database.
    ///
    /// `requested_scopes` are the scopes this listing operation needs (e.g.
    /// `{read:metadata}` for a search, `{read:data}` for a full export).
    /// A policy may deny the listing outright (an all-false filter) when the
    /// caller's request exceeds what it can grant, and may widen the public
    /// surface only for read-scoped listings. Mirrors the `scopes` argument
    /// of Python `AccessPolicy.filters` (access_policies.py:368-413).
    async fn list_filter(
        &self,
        _principal: Option<&Principal>,
        _session_scopes: &ScopeSet,
        _requested_scopes: &ScopeSet,
    ) -> Option<AccessBlobFilter> {
        None
    }
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

    /// The owning principal UUID if this node carries a `{"user": "..."}`
    /// claim. A user-owned node is private to that owner: it has no `tags`
    /// key, so it must be gated on ownership rather than treated as an
    /// untagged-public node (otherwise every caller could read it).
    fn node_owner<'a>(ctx: &NodeContext<'a>) -> Option<&'a str> {
        ctx.access_blob.get("user").and_then(|v| v.as_str())
    }
}

#[async_trait]
impl AccessPolicy for TagBasedPolicy {
    async fn anonymous_decision(&self, ctx: NodeContext<'_>) -> Decision {
        // A user-owned node ({"user": id}) is private to its owner. Anonymous
        // never owns a node, so even though such a blob carries no `tags` key
        // it must NOT be treated as untagged-public and exposed anonymously.
        if Self::node_owner(&ctx).is_some() {
            return Decision {
                scopes: ScopeSet::default(),
            };
        }
        // Anonymous sees untagged nodes and nodes carrying the literal
        // "public" tag (world-readable; mirrors Python is_tag_public for the
        // built-in public_tag, access_policies.py:354-356).
        let node_tags = Self::node_tags(&ctx);
        if node_tags.is_empty() || node_tags.iter().any(|t| t == PUBLIC_TAG) {
            Decision {
                scopes: ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData]),
            }
        } else {
            Decision {
                scopes: ScopeSet::default(),
            }
        }
    }

    async fn principal_decision(
        &self,
        principal: &Principal,
        session_scopes: &ScopeSet,
        ctx: NodeContext<'_>,
    ) -> Decision {
        // Admin short-circuit: admins keep their full session scopes,
        // unrestricted by ownership or tags. Mirrors the admin branch of
        // Python allowed_scopes (access_policies.py:335-336).
        if session_scopes.contains(Scope::AdminApiKeys) {
            return Decision {
                scopes: session_scopes.clone(),
            };
        }
        // User-owned nodes are private to their owner. A {"user": id} blob has
        // no `tags` key, so the tags path below would otherwise treat it as
        // untagged-public and leak it to every principal.
        if let Some(owner) = Self::node_owner(&ctx) {
            let scopes = if owner == principal.uuid {
                session_scopes.intersect(&self.default_scopes)
            } else {
                ScopeSet::default()
            };
            return Decision { scopes };
        }
        let node_tags = Self::node_tags(&ctx);
        let granted = self
            .principal_tags
            .get(&principal.uuid)
            .cloned()
            .unwrap_or_default();
        // Untagged nodes and granted-tag matches confer the policy's default
        // scopes; the literal "public" tag confers read scopes to everyone
        // (is_tag_public, access_policies.py:354-356). Union the two grants,
        // each already capped at the principal's session (never widened).
        let granted_or_untagged =
            node_tags.is_empty() || node_tags.iter().any(|t| granted.contains(t));
        let mut scopes = if granted_or_untagged {
            session_scopes.intersect(&self.default_scopes)
        } else {
            ScopeSet::default()
        };
        if node_tags.iter().any(|t| t == PUBLIC_TAG) {
            for s in session_scopes.intersect(&ScopeSet::read_only()).iter() {
                scopes.insert(s);
            }
        }
        Decision { scopes }
    }

    async fn list_filter(
        &self,
        principal: Option<&Principal>,
        session_scopes: &ScopeSet,
        requested_scopes: &ScopeSet,
    ) -> Option<AccessBlobFilter> {
        // NO_ACCESS: the requested scopes must be grantable by this policy.
        // Mirrors Python filters() line 378-379
        // (`if not scopes.issubset(self.scopes): return NO_ACCESS`).
        // ALL_ACCESS is `None`; NO_ACCESS is an all-false filter (deny-all).
        if !requested_scopes.0.is_subset(&self.default_scopes.0) {
            return Some(AccessBlobFilter::default());
        }

        // Admin short-circuit → ALL_ACCESS. Admins are not narrowed by tags.
        // Mirrors filters() line 387-388 (`elif self._is_admin: return ALL_ACCESS`).
        if principal.is_some() && session_scopes.contains(Scope::AdminApiKeys) {
            return None;
        }

        let (user_id, mut tags) = match principal {
            None => (None, vec![]),
            Some(p) => {
                // Each granted tag confers `default_scopes` in this built-in,
                // so Python's per-scope tag intersection
                // (`∩ get_tags_from_scope(scope, id)` over the requested
                // scopes, filters() line 391-398) reduces to the principal's
                // full granted-tag set once the subset gate above confirmed
                // every requested scope is grantable.
                let granted = self
                    .principal_tags
                    .get(&p.uuid)
                    .cloned()
                    .unwrap_or_default();
                (Some(p.uuid.clone()), granted)
            }
        };

        // The public surface is read-only: the untagged rows and the literal
        // "public" tag appear only when every requested scope is a read scope.
        // Mirrors filters() line 400-407 (`get_public_tags()` added only for
        // `scope in read_scopes`). Adding PUBLIC_TAG here keeps the list filter
        // consistent with the per-node "public"-tag read grant — a
        // "public"-tagged node is both listable and readable by everyone. A
        // write-scoped listing exposes neither.
        let include_untagged = requested_scopes.0.is_subset(&ScopeSet::read_only().0);
        if include_untagged && !tags.iter().any(|t| t == PUBLIC_TAG) {
            tags.push(PUBLIC_TAG.to_string());
        }

        Some(AccessBlobFilter {
            user_id,
            tags,
            include_untagged,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(uuid: &str) -> Principal {
        serde_json::from_value(serde_json::json!({
            "id": 1,
            "uuid": uuid,
            "type": "user",
            "role": "user",
            "time_created": "2020-01-01T00:00:00Z",
        }))
        .unwrap()
    }

    fn read_metadata() -> ScopeSet {
        ScopeSet::from_iter([Scope::ReadMetadata])
    }

    #[tokio::test]
    async fn list_filter_anonymous_lists_untagged_and_public_tag() {
        let policy = TagBasedPolicy::new(ScopeSet::full());
        let f = policy
            .list_filter(None, &ScopeSet::default(), &read_metadata())
            .await
            .unwrap();
        assert_eq!(f.user_id, None);
        // Read listing surfaces both untagged rows and the literal "public" tag.
        assert_eq!(f.tags, vec!["public".to_string()]);
        assert!(f.include_untagged);
    }

    #[tokio::test]
    async fn list_filter_principal_includes_granted_owned_and_public() {
        let mut policy = TagBasedPolicy::new(ScopeSet::full());
        policy.grant("alice", "team-a");
        let alice = principal("alice");
        let session = ScopeSet::for_role("user");
        let f = policy
            .list_filter(Some(&alice), &session, &read_metadata())
            .await
            .unwrap();
        assert_eq!(f.user_id.as_deref(), Some("alice"));
        assert!(f.tags.contains(&"team-a".to_string()));
        assert!(f.tags.contains(&"public".to_string()));
        assert!(f.include_untagged);
    }

    #[tokio::test]
    async fn list_filter_admin_gets_all_access() {
        let policy = TagBasedPolicy::new(ScopeSet::full());
        let admin = principal("admin-uuid");
        // An admin session (role "admin" → full scopes) carries admin:apikeys.
        let session = ScopeSet::full();
        let f = policy
            .list_filter(Some(&admin), &session, &read_metadata())
            .await;
        assert!(f.is_none(), "admin must get ALL_ACCESS (no row filter)");
    }

    #[tokio::test]
    async fn list_filter_denies_when_requested_scope_exceeds_policy() {
        // Policy can grant only read scopes; a write listing is NO_ACCESS.
        let policy = TagBasedPolicy::new(ScopeSet::read_only());
        let alice = principal("alice");
        let session = ScopeSet::for_role("user");
        let write = ScopeSet::from_iter([Scope::WriteData]);
        let f = policy
            .list_filter(Some(&alice), &session, &write)
            .await
            .unwrap();
        // Deny-all: no user arm, no tags, no untagged-public arm.
        assert_eq!(f.user_id, None);
        assert!(f.tags.is_empty());
        assert!(!f.include_untagged);
    }

    #[tokio::test]
    async fn list_filter_write_scope_excludes_untagged_public() {
        let mut policy = TagBasedPolicy::new(ScopeSet::full());
        policy.grant("alice", "team-a");
        let alice = principal("alice");
        let session = ScopeSet::for_role("user");
        let write = ScopeSet::from_iter([Scope::WriteData]);
        let f = policy
            .list_filter(Some(&alice), &session, &write)
            .await
            .unwrap();
        // Owned + granted rows still match, but untagged-public and the
        // "public" tag must NOT be exposed on a non-read listing.
        assert_eq!(f.user_id.as_deref(), Some("alice"));
        assert_eq!(f.tags, vec!["team-a".to_string()]);
        assert!(!f.include_untagged);
    }

    fn ctx<'a>(access_blob: &'a serde_json::Value, meta: &'a serde_json::Value) -> NodeContext<'a> {
        NodeContext {
            path: &[],
            structure_family: "container",
            metadata: meta,
            access_blob,
        }
    }

    #[tokio::test]
    async fn anonymous_decision_reads_public_tagged_node() {
        let policy = TagBasedPolicy::new(ScopeSet::full());
        let blob = serde_json::json!({"tags": ["public"]});
        let meta = serde_json::json!({});
        let d = policy.anonymous_decision(ctx(&blob, &meta)).await;
        assert!(d.scopes.contains(Scope::ReadMetadata));
        assert!(d.scopes.contains(Scope::ReadData));
    }

    #[tokio::test]
    async fn anonymous_decision_hides_non_public_tagged_node() {
        let policy = TagBasedPolicy::new(ScopeSet::full());
        let blob = serde_json::json!({"tags": ["secret"]});
        let meta = serde_json::json!({});
        let d = policy.anonymous_decision(ctx(&blob, &meta)).await;
        assert!(!d.scopes.contains(Scope::ReadMetadata));
    }

    #[tokio::test]
    async fn principal_decision_admin_bypasses_tags_and_ownership() {
        let policy = TagBasedPolicy::new(ScopeSet::read_only());
        let admin = principal("admin-uuid");
        let session = ScopeSet::full(); // admin session carries admin:apikeys
        // A node owned by someone else and tagged with an ungranted tag.
        let blob = serde_json::json!({"user": "other-uuid", "tags": ["secret"]});
        let meta = serde_json::json!({});
        let d = policy
            .principal_decision(&admin, &session, ctx(&blob, &meta))
            .await;
        // Admin keeps the full session scopes, unrestricted.
        assert_eq!(d.scopes, session);
    }

    #[tokio::test]
    async fn principal_decision_reads_public_tagged_node_without_grant() {
        // A non-admin principal with no matching tag grant still reads a
        // "public"-tagged node (read scopes only).
        let policy = TagBasedPolicy::new(ScopeSet::full());
        let bob = principal("bob");
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["public"]});
        let meta = serde_json::json!({});
        let d = policy
            .principal_decision(&bob, &session, ctx(&blob, &meta))
            .await;
        assert!(d.scopes.contains(Scope::ReadMetadata));
        assert!(d.scopes.contains(Scope::ReadData));
        // Public-tag grant is read-only: no write scope even though the
        // session and default scopes include write.
        assert!(!d.scopes.contains(Scope::WriteData));
    }

    #[tokio::test]
    async fn principal_decision_hides_ungranted_non_public_tag() {
        let policy = TagBasedPolicy::new(ScopeSet::full());
        let bob = principal("bob");
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["secret"]});
        let meta = serde_json::json!({});
        let d = policy
            .principal_decision(&bob, &session, ctx(&blob, &meta))
            .await;
        assert!(!d.scopes.contains(Scope::ReadMetadata));
    }
}
