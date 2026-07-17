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

use crate::core::queries::AccessBlobFilter;
use async_trait::async_trait;

pub use crate::auth::{Principal, Scope, ScopeSet};

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
        authn_access_tags: Option<&[String]>,
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
        _authn_access_tags: Option<&[String]>,
    ) -> Option<AccessBlobFilter> {
        None
    }

    /// Called when a new node is being created. The policy may validate
    /// and/or replace the client-supplied (or server-generated) `access_blob`.
    /// Returns `(modified, final_blob)`:
    ///   - `modified = true` → the returned blob differs from the input.
    ///   - `modified = false` → the blob is used as-is.
    ///
    /// Errors are surfaced as HTTP 422 (validation error), mirroring Python's
    /// `ValueError` path in `TagBasedAccessPolicy.init_node`
    /// (access_policies.py:108-193).
    ///
    /// Default: pass the supplied blob through unchanged (no policy
    /// validation). Mirrors `DummyAccessPolicy.init_node`.
    async fn init_node(
        &self,
        _principal: &Principal,
        _authn_access_tags: Option<&[String]>,
        _session_scopes: &ScopeSet,
        access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        Ok((
            false,
            access_blob
                .cloned()
                .unwrap_or(serde_json::Value::Object(Default::default())),
        ))
    }

    /// Called when a node's `access_blob` is being updated via PATCH/PUT.
    /// Returns `(modified, final_blob)`:
    ///   - `modified = true` → the returned blob should replace the stored one.
    ///   - `modified = false` → the stored blob is kept unchanged.
    ///
    /// `node_access_blob` is the current stored blob; `proposed_access_blob` is
    /// what the client sent. Errors are surfaced as HTTP 422.
    ///
    /// Default: keep the current blob unchanged (no mutation). Mirrors the base
    /// `modify_node` in `AccessPolicy` (protocols.py:20-28).
    async fn modify_node(
        &self,
        node_access_blob: &serde_json::Value,
        _principal: &Principal,
        _authn_access_tags: Option<&[String]>,
        _session_scopes: &ScopeSet,
        _proposed_access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        Ok((false, node_access_blob.clone()))
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
        _authn_access_tags: Option<&[String]>,
        _ctx: NodeContext<'_>,
    ) -> Decision {
        // Passthrough trusts the session verbatim and is intentionally
        // tag-agnostic: an API key's `authn_access_tags` restriction is only
        // meaningful to a tag-aware policy, so it is ignored here.
        Decision {
            scopes: session_scopes.clone(),
        }
    }
}

/// Built-in: tag-based policy backed by the auth database.
///
/// A principal's access_tags are stored in the `principals.access_tags` DB
/// column (a JSON array of tag strings). A node is visible iff its
/// `access_blob.tags` intersects the principal's DB-stored tag set, OR the
/// node has no tags (treated as public). "public" is universally readable.
pub struct TagBasedPolicy {
    /// Auth DB used to load per-principal tag grants at request time.
    pub auth_db: std::sync::Arc<crate::auth::AuthDb>,
    /// Default scopes when a tagged node matches.
    pub default_scopes: ScopeSet,
}

impl TagBasedPolicy {
    pub fn new(auth_db: std::sync::Arc<crate::auth::AuthDb>, default_scopes: ScopeSet) -> Self {
        Self {
            auth_db,
            default_scopes,
        }
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

    /// Narrow a principal's granted tag set by an API key's `authn_access_tags`
    /// restriction: the effective grant is the intersection. `None` (no key
    /// restriction) leaves the set unchanged. This is the SINGLE owner of the
    /// key-narrowing rule, shared by `list_filter` (listing visibility) and
    /// `principal_decision` (per-node direct-access gate) so the two stay
    /// uniform — a key cannot reach by direct path what it cannot see in a
    /// listing. Mirrors Python access_policies.py:409-411
    /// (`access_tags = access_tags & authn_access_tags`).
    fn narrow_by_key(granted: &mut Vec<String>, authn_access_tags: Option<&[String]>) {
        if let Some(key_tags) = authn_access_tags {
            let key_set: std::collections::HashSet<&str> =
                key_tags.iter().map(|s| s.as_str()).collect();
            granted.retain(|t| key_set.contains(t.as_str()));
        }
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
        authn_access_tags: Option<&[String]>,
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
        let mut granted = self
            .auth_db
            .get_principal_tags(&principal.uuid)
            .await
            .unwrap_or_default();
        // authn_access_tags narrowing: an API key's tag restriction narrows the
        // principal's effective grant to the intersection — the SAME rule
        // list_filter applies (via narrow_by_key), so this per-node gate matches
        // listing visibility and a narrow key cannot reach an out-of-tag node by
        // direct path. Untagged/public/owned grants below are intentionally NOT
        // narrowed (uniform with list_filter's include_untagged + public tag).
        Self::narrow_by_key(&mut granted, authn_access_tags);
        // Compute effective scopes as union of per-tag scope grants.
        // Untagged nodes: grant default_scopes to everyone (backward-compat).
        // "public" tag: grants read_only scopes to everyone.
        // For each other node tag the principal owns: look up tag_scopes;
        // if no rows exist fall back to default_scopes (backward-compat).
        // Mirrors access_policies.py allowed_scopes (354-370).
        let mut effective = ScopeSet::default();
        if node_tags.is_empty() {
            for s in self.default_scopes.iter() {
                effective.insert(s);
            }
        } else {
            for tag in &node_tags {
                if tag == PUBLIC_TAG {
                    for s in ScopeSet::read_only().iter() {
                        effective.insert(s);
                    }
                } else if granted.contains(tag) {
                    let scope_strs = self.auth_db.get_tag_scopes(tag).await.unwrap_or_default();
                    let tag_scopes = if scope_strs.is_empty() {
                        self.default_scopes.clone()
                    } else {
                        ScopeSet::from_iter(scope_strs.iter().filter_map(|s| Scope::parse(s)))
                    };
                    for s in tag_scopes.iter() {
                        effective.insert(s);
                    }
                }
            }
        }
        Decision {
            scopes: session_scopes.intersect(&effective),
        }
    }

    async fn init_node(
        &self,
        principal: &Principal,
        authn_access_tags: Option<&[String]>,
        session_scopes: &ScopeSet,
        access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        let is_admin = session_scopes.contains(Scope::AdminApiKeys);

        let Some(blob) = access_blob else {
            // No blob: default to user-owned node ({"user": uuid}).
            // Mirrors access_policies.py:179-187.
            if let Some(key_tags) = authn_access_tags {
                return Err(format!(
                    "Cannot init node as user-owned node.\n\
                     Current API key does not permit action on user-owned nodes.\n\
                     Please provide a tag allowed by this API key: {:?}",
                    key_tags
                ));
            }
            return Ok((true, serde_json::json!({"user": principal.uuid})));
        };

        // Validate blob shape: must be exactly {"tags": [...]}.
        // Mirrors access_policies.py:121-125.
        let blob_obj = blob
            .as_object()
            .filter(|m| m.len() == 1 && m.contains_key("tags"))
            .ok_or_else(|| {
                format!(
                    "access_blob must be in the form {{\"tags\": [\"tag1\", \"tag2\", ...]}}\nReceived {blob}"
                )
            })?;

        let tag_arr = blob_obj["tags"]
            .as_array()
            .ok_or_else(|| format!("access_blob.tags must be an array, received: {blob}"))?;

        let access_tags: Vec<String> = tag_arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        // Empty tag list is admin-only. Mirrors access_policies.py:126-130.
        if access_tags.is_empty() && !is_admin {
            return Err(
                "Cannot apply empty tag list to node: only Tiled admins can apply an empty tag list."
                    .to_string(),
            );
        }

        // Load granted tags only for non-admin; admin bypasses ownership.
        let granted: Vec<String> = if is_admin {
            vec![]
        } else {
            self.auth_db
                .get_principal_tags(&principal.uuid)
                .await
                .map_err(|e| format!("failed to load principal tags: {e}"))?
        };

        let mut include_public_tag = false;

        // Per-tag checks. Mirrors access_policies.py:133-156.
        for tag in &access_tags {
            // API key restriction applies to all tags, including for admins.
            // Checked first, before is_tag_defined, matching Python order.
            if let Some(key_tags) = authn_access_tags
                && !key_tags.iter().any(|k| k == tag)
            {
                return Err(format!(
                    "Cannot apply tag to node: API key is restricted to access tags: {:?}.",
                    key_tags
                ));
            }

            if tag.to_lowercase() == PUBLIC_TAG {
                include_public_tag = true;
                if !is_admin {
                    return Err(
                        "Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                            .to_string(),
                    );
                }
            } else {
                // is_tag_defined: ALL principals (including admin) must only
                // assign tags that exist in the registry.
                // Mirrors access_policies.py:145-146.
                let defined = self
                    .auth_db
                    .is_tag_defined(tag)
                    .await
                    .map_err(|e| format!("failed to check tag registry: {e}"))?;
                if !defined {
                    return Err(format!(
                        "Cannot apply tag to node: tag={tag:?} is not defined"
                    ));
                }
                // Ownership check — admin bypasses. Mirrors access_policies.py:147-152.
                if !is_admin && !granted.contains(tag) {
                    return Err(format!(
                        "Cannot apply tag to node: user='{}' is not an owner of tag={tag:?}",
                        principal.uuid
                    ));
                }
            }
        }

        // Build the policy-normalized tag set (canonical lowercase "public").
        // Mirrors access_policies.py:158-165.
        let mut tags_from_policy: Vec<String> = access_tags
            .iter()
            .filter(|t| t.to_lowercase() != PUBLIC_TAG)
            .cloned()
            .collect();
        if include_public_tag {
            tags_from_policy.push(PUBLIC_TAG.to_string());
        }

        // Unremovable scopes: non-admin must retain read:metadata + write:metadata
        // on the new node after tag assignment. Mirrors access_policies.py:168-177.
        if !is_admin {
            let mut effective = ScopeSet::default();
            for tag in &tags_from_policy {
                if tag == PUBLIC_TAG {
                    for s in ScopeSet::read_only().iter() {
                        effective.insert(s);
                    }
                } else {
                    let scope_strs = self.auth_db.get_tag_scopes(tag).await.unwrap_or_default();
                    let tag_scopes = if scope_strs.is_empty() {
                        self.default_scopes.clone()
                    } else {
                        ScopeSet::from_iter(scope_strs.iter().filter_map(|s| Scope::parse(s)))
                    };
                    for s in tag_scopes.iter() {
                        effective.insert(s);
                    }
                }
            }
            if !effective.contains(Scope::ReadMetadata) || !effective.contains(Scope::WriteMetadata)
            {
                return Err(
                    "Cannot init node: tag configuration would remove a required scope \
                     (read:metadata and write:metadata must remain accessible)"
                        .to_string(),
                );
            }
        }

        let input_set: std::collections::HashSet<&str> =
            access_tags.iter().map(String::as_str).collect();
        let output_set: std::collections::HashSet<&str> =
            tags_from_policy.iter().map(String::as_str).collect();
        let modified = input_set != output_set;

        Ok((modified, serde_json::json!({"tags": tags_from_policy})))
    }

    async fn modify_node(
        &self,
        node_access_blob: &serde_json::Value,
        principal: &Principal,
        authn_access_tags: Option<&[String]>,
        session_scopes: &ScopeSet,
        proposed_access_blob: Option<&serde_json::Value>,
    ) -> Result<(bool, serde_json::Value), String> {
        let is_admin = session_scopes.contains(Scope::AdminApiKeys);

        let Some(proposed) = proposed_access_blob else {
            return Ok((false, node_access_blob.clone()));
        };

        // No-op: proposed matches current. Mirrors access_policies.py:208-212.
        if proposed == node_access_blob {
            return Ok((false, node_access_blob.clone()));
        }

        // Validate proposed blob shape. Mirrors access_policies.py:214-219.
        let blob_obj = proposed
            .as_object()
            .filter(|m| m.len() == 1 && m.contains_key("tags"))
            .ok_or_else(|| {
                format!(
                    "access_blob must be in the form {{\"tags\": [\"tag1\", \"tag2\", ...]}}\n\
                     Received {proposed}\n\
                     If this was a merge patch on a user-owned node, use a replace op instead."
                )
            })?;

        let tag_arr = blob_obj["tags"]
            .as_array()
            .ok_or_else(|| format!("access_blob.tags must be an array, received: {proposed}"))?;

        let access_tags: Vec<String> = tag_arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        // Empty tag list is admin-only. Mirrors access_policies.py:220-223.
        if access_tags.is_empty() && !is_admin {
            return Err(
                "Cannot apply empty tag list to node: only Tiled admins can apply an empty tag list."
                    .to_string(),
            );
        }

        // Current tags from node (empty if blob is {"user": id}).
        let current_tags: Vec<String> = node_access_blob
            .get("tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let granted: Vec<String> = if is_admin {
            vec![]
        } else {
            self.auth_db
                .get_principal_tags(&principal.uuid)
                .await
                .map_err(|e| format!("failed to load principal tags: {e}"))?
        };

        let mut include_public_tag = false;

        // Check proposed tags (being added or kept). Mirrors access_policies.py:227-259.
        for tag in &access_tags {
            // API key restriction applies to all proposed tags (even admins).
            if let Some(key_tags) = authn_access_tags
                && !key_tags.iter().any(|k| k == tag)
            {
                return Err(format!(
                    "Cannot apply tag to node: API key is restricted to access tags: {:?}.",
                    key_tags
                ));
            }

            if current_tags.contains(tag) {
                // Already on node — ownership/definition check not required.
                include_public_tag = include_public_tag || (tag.to_lowercase() == PUBLIC_TAG);
                continue;
            }

            if tag.to_lowercase() == PUBLIC_TAG {
                include_public_tag = true;
                if !is_admin {
                    return Err(
                        "Cannot apply 'public' tag to node: only Tiled admins can apply the 'public' tag."
                            .to_string(),
                    );
                }
            } else {
                // is_tag_defined: all principals (including admin) may only
                // assign defined tags. Mirrors access_policies.py:248-249.
                let defined = self
                    .auth_db
                    .is_tag_defined(tag)
                    .await
                    .map_err(|e| format!("failed to check tag registry: {e}"))?;
                if !defined {
                    return Err(format!(
                        "Cannot apply tag to node: tag={tag:?} is not defined"
                    ));
                }
                // Ownership check — admin bypasses. Mirrors access_policies.py:250-255.
                if !is_admin && !granted.contains(tag) {
                    return Err(format!(
                        "Cannot apply tag to node: user='{}' is not an owner of tag={tag:?}",
                        principal.uuid
                    ));
                }
            }
        }

        // Build policy-normalized tag set. Mirrors access_policies.py:261-265.
        let mut tags_from_policy: Vec<String> = access_tags
            .iter()
            .filter(|t| t.to_lowercase() != PUBLIC_TAG)
            .cloned()
            .collect();
        if include_public_tag {
            tags_from_policy.push(PUBLIC_TAG.to_string());
        }

        let policy_set: std::collections::HashSet<&str> =
            tags_from_policy.iter().map(String::as_str).collect();

        // Check tags being removed (in current blob, not in normalized proposed).
        // Mirrors access_policies.py:267-296.
        for tag in &current_tags {
            if policy_set.contains(tag.as_str()) {
                continue; // kept
            }

            // API key restriction applies to removed tags too.
            if let Some(key_tags) = authn_access_tags
                && !key_tags.iter().any(|k| k == tag)
            {
                return Err(format!(
                    "Cannot remove tag from node: API key is restricted to access tags: {:?}.",
                    key_tags
                ));
            }

            if tag == PUBLIC_TAG {
                if !is_admin {
                    return Err(
                        "Cannot remove 'public' tag from node: only Tiled admins can remove the 'public' tag."
                            .to_string(),
                    );
                }
            } else {
                // is_tag_defined for removal too. Mirrors access_policies.py:283-285.
                let defined = self
                    .auth_db
                    .is_tag_defined(tag)
                    .await
                    .map_err(|e| format!("failed to check tag registry: {e}"))?;
                if !defined {
                    return Err(format!(
                        "Cannot remove tag from node: tag={tag:?} is not defined"
                    ));
                }
                // Ownership check — admin bypasses. Mirrors access_policies.py:286-291.
                if !is_admin && !granted.contains(tag) {
                    return Err(format!(
                        "Cannot remove tag from node: user='{}' is not an owner of tag={tag:?}",
                        principal.uuid
                    ));
                }
            }
        }

        // Unremovable scopes: non-admin must retain read:metadata + write:metadata
        // after the tag change. Mirrors access_policies.py:296-310.
        if !is_admin {
            let mut effective = ScopeSet::default();
            for tag in &tags_from_policy {
                if tag == PUBLIC_TAG {
                    for s in ScopeSet::read_only().iter() {
                        effective.insert(s);
                    }
                } else {
                    let scope_strs = self.auth_db.get_tag_scopes(tag).await.unwrap_or_default();
                    let tag_scopes = if scope_strs.is_empty() {
                        self.default_scopes.clone()
                    } else {
                        ScopeSet::from_iter(scope_strs.iter().filter_map(|s| Scope::parse(s)))
                    };
                    for s in tag_scopes.iter() {
                        effective.insert(s);
                    }
                }
            }
            if !effective.contains(Scope::ReadMetadata) || !effective.contains(Scope::WriteMetadata)
            {
                return Err("Cannot change node tags: would remove a required scope \
                     (read:metadata and write:metadata must remain accessible)"
                    .to_string());
            }
        }

        let access_tags_set: std::collections::HashSet<&str> =
            access_tags.iter().map(String::as_str).collect();
        let modified = access_tags_set != policy_set;

        Ok((modified, serde_json::json!({"tags": tags_from_policy})))
    }

    async fn list_filter(
        &self,
        principal: Option<&Principal>,
        session_scopes: &ScopeSet,
        requested_scopes: &ScopeSet,
        authn_access_tags: Option<&[String]>,
    ) -> Option<AccessBlobFilter> {
        // NO_ACCESS: the requested scopes must be grantable by this policy.
        // Mirrors Python filters() line 378-379
        // (`if not scopes.issubset(self.scopes): return NO_ACCESS`).
        // ALL_ACCESS is `None`; NO_ACCESS is an all-false filter (deny-all).
        if !requested_scopes.is_subset(&self.default_scopes) {
            return Some(AccessBlobFilter::default());
        }

        // Admin short-circuit → ALL_ACCESS. Admins are not narrowed by tags.
        // Mirrors filters() line 387-388 (`elif self._is_admin: return ALL_ACCESS`).
        if principal.is_some() && session_scopes.contains(Scope::AdminApiKeys) {
            return None;
        }

        let (user_id, granted) = match principal {
            None => (None, vec![]),
            Some(p) => {
                let mut granted = self
                    .auth_db
                    .get_principal_tags(&p.uuid)
                    .await
                    .unwrap_or_default();
                // authn_access_tags narrowing — the shared rule (see
                // narrow_by_key): the effective grant is the principal's DB tags
                // ∩ the key's tags. principal_decision applies the identical
                // narrowing so listing and per-node gates stay uniform.
                Self::narrow_by_key(&mut granted, authn_access_tags);
                (Some(p.uuid.clone()), granted)
            }
        };

        // Per-tag scope filtering: include only tags that grant ALL requested
        // scopes. Tags with no tag_scopes rows fall back to default_scopes.
        // The NO_ACCESS guard above confirmed requested_scopes ⊆ default_scopes,
        // so fallback tags always pass (backward-compat: empty tag_scopes = same
        // behaviour as before migration 0005). Mirrors Python
        // `get_tags_from_scope` / set intersection (filters() line 391-398).
        let mut tags: Vec<String> = Vec::new();
        for t in granted {
            let scope_strs = self.auth_db.get_tag_scopes(&t).await.unwrap_or_default();
            let effective_tag_scopes = if scope_strs.is_empty() {
                self.default_scopes.clone()
            } else {
                ScopeSet::from_iter(scope_strs.iter().filter_map(|s| Scope::parse(s)))
            };
            if requested_scopes.is_subset(&effective_tag_scopes) {
                tags.push(t);
            }
        }

        // The public surface is read-only: the untagged rows and the literal
        // "public" tag appear only when every requested scope is a read scope.
        // Mirrors filters() line 400-407 (`get_public_tags()` added only for
        // `scope in read_scopes`). Adding PUBLIC_TAG here keeps the list filter
        // consistent with the per-node "public"-tag read grant — a
        // "public"-tagged node is both listable and readable by everyone. A
        // write-scoped listing exposes neither.
        let include_untagged = requested_scopes.is_subset(&ScopeSet::read_only());
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
    use std::sync::Arc;

    use super::*;

    fn principal_from(p: &crate::auth::Principal) -> Principal {
        p.clone()
    }

    fn admin_like(uuid: &str) -> Principal {
        serde_json::from_value(serde_json::json!({
            "id": 999,
            "uuid": uuid,
            "type": "user",
            "role": "admin",
            "time_created": "2020-01-01T00:00:00Z",
        }))
        .unwrap()
    }

    fn read_metadata() -> ScopeSet {
        ScopeSet::from_iter([Scope::ReadMetadata])
    }

    async fn setup_auth_db() -> crate::auth::AuthDb {
        let db = crate::auth::AuthDb::connect("sqlite::memory:")
            .await
            .expect("in-memory sqlite");
        db.migrate().await.expect("migrations");
        db
    }

    async fn principal_with_tags(
        db: &crate::auth::AuthDb,
        tags: &[&str],
    ) -> crate::auth::Principal {
        let p = db.create_principal("user").await.expect("create principal");
        let tag_strs: Vec<String> = tags.iter().map(|s| s.to_string()).collect();
        db.set_principal_tags(p.id, &tag_strs)
            .await
            .expect("set tags");
        p
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
    async fn list_filter_anonymous_lists_untagged_and_public_tag() {
        let db = setup_auth_db().await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let f = policy
            .list_filter(None, &ScopeSet::default(), &read_metadata(), None)
            .await
            .unwrap();
        assert_eq!(f.user_id, None);
        assert_eq!(f.tags, vec!["public".to_string()]);
        assert!(f.include_untagged);
    }

    #[tokio::test]
    async fn list_filter_principal_includes_granted_owned_and_public() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let f = policy
            .list_filter(
                Some(&principal_from(&alice)),
                &session,
                &read_metadata(),
                None,
            )
            .await
            .unwrap();
        assert_eq!(f.user_id.as_deref(), Some(alice.uuid.as_str()));
        assert!(f.tags.contains(&"team-a".to_string()));
        assert!(f.tags.contains(&"public".to_string()));
        assert!(f.include_untagged);
    }

    #[tokio::test]
    async fn list_filter_admin_gets_all_access() {
        let db = setup_auth_db().await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let admin = admin_like("admin-uuid");
        let session = ScopeSet::full();
        let f = policy
            .list_filter(Some(&admin), &session, &read_metadata(), None)
            .await;
        assert!(f.is_none(), "admin must get ALL_ACCESS (no row filter)");
    }

    #[tokio::test]
    async fn list_filter_denies_when_requested_scope_exceeds_policy() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::read_only());
        let session = ScopeSet::for_role("user");
        let write = ScopeSet::from_iter([Scope::WriteData]);
        let f = policy
            .list_filter(Some(&principal_from(&alice)), &session, &write, None)
            .await
            .unwrap();
        assert_eq!(f.user_id, None);
        assert!(f.tags.is_empty());
        assert!(!f.include_untagged);
    }

    #[tokio::test]
    async fn list_filter_write_scope_excludes_untagged_public() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let write = ScopeSet::from_iter([Scope::WriteData]);
        let f = policy
            .list_filter(Some(&principal_from(&alice)), &session, &write, None)
            .await
            .unwrap();
        assert_eq!(f.user_id.as_deref(), Some(alice.uuid.as_str()));
        assert_eq!(f.tags, vec!["team-a".to_string()]);
        assert!(!f.include_untagged);
    }

    // ---- init_node / modify_node default behaviour ----

    #[tokio::test]
    async fn init_node_default_passes_blob_through() {
        let policy = PassthroughPolicy;
        let db = setup_auth_db().await;
        let p = db.create_principal("user").await.unwrap();
        let session = ScopeSet::for_role("user");
        let proposed = serde_json::json!({"user": p.uuid});
        let (modified, result) = policy
            .init_node(&p, None, &session, Some(&proposed))
            .await
            .unwrap();
        assert!(!modified, "default init_node must not signal modification");
        assert_eq!(
            result, proposed,
            "default init_node must return supplied blob"
        );
    }

    #[tokio::test]
    async fn init_node_default_empty_blob_produces_empty_object() {
        let policy = PassthroughPolicy;
        let db = setup_auth_db().await;
        let p = db.create_principal("user").await.unwrap();
        let session = ScopeSet::for_role("user");
        let (modified, result) = policy.init_node(&p, None, &session, None).await.unwrap();
        assert!(!modified);
        assert_eq!(result, serde_json::Value::Object(Default::default()));
    }

    #[tokio::test]
    async fn modify_node_default_keeps_current_blob() {
        let policy = PassthroughPolicy;
        let db = setup_auth_db().await;
        let p = db.create_principal("user").await.unwrap();
        let session = ScopeSet::for_role("user");
        let current = serde_json::json!({"user": p.uuid});
        let proposed = serde_json::json!({"tags": ["secret"]});
        let (modified, result) = policy
            .modify_node(&current, &p, None, &session, Some(&proposed))
            .await
            .unwrap();
        assert!(
            !modified,
            "default modify_node must not signal modification"
        );
        assert_eq!(
            result, current,
            "default modify_node must return the current node blob, not the proposed one"
        );
    }

    #[tokio::test]
    async fn anonymous_decision_reads_public_tagged_node() {
        let db = setup_auth_db().await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let blob = serde_json::json!({"tags": ["public"]});
        let meta = serde_json::json!({});
        let d = policy.anonymous_decision(ctx(&blob, &meta)).await;
        assert!(d.scopes.contains(Scope::ReadMetadata));
        assert!(d.scopes.contains(Scope::ReadData));
    }

    #[tokio::test]
    async fn anonymous_decision_hides_non_public_tagged_node() {
        let db = setup_auth_db().await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let blob = serde_json::json!({"tags": ["secret"]});
        let meta = serde_json::json!({});
        let d = policy.anonymous_decision(ctx(&blob, &meta)).await;
        assert!(!d.scopes.contains(Scope::ReadMetadata));
    }

    #[tokio::test]
    async fn principal_decision_admin_bypasses_tags_and_ownership() {
        let db = setup_auth_db().await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::read_only());
        let admin = admin_like("admin-uuid");
        let session = ScopeSet::full();
        let blob = serde_json::json!({"user": "other-uuid", "tags": ["secret"]});
        let meta = serde_json::json!({});
        let d = policy
            .principal_decision(&admin, &session, None, ctx(&blob, &meta))
            .await;
        assert_eq!(d.scopes, session);
    }

    #[tokio::test]
    async fn principal_decision_reads_public_tagged_node_without_grant() {
        let db = setup_auth_db().await;
        let bob = principal_with_tags(&db, &[]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["public"]});
        let meta = serde_json::json!({});
        let d = policy
            .principal_decision(&principal_from(&bob), &session, None, ctx(&blob, &meta))
            .await;
        assert!(d.scopes.contains(Scope::ReadMetadata));
        assert!(d.scopes.contains(Scope::ReadData));
        assert!(!d.scopes.contains(Scope::WriteData));
    }

    #[tokio::test]
    async fn principal_decision_hides_ungranted_non_public_tag() {
        let db = setup_auth_db().await;
        let bob = principal_with_tags(&db, &[]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["secret"]});
        let meta = serde_json::json!({});
        let d = policy
            .principal_decision(&principal_from(&bob), &session, None, ctx(&blob, &meta))
            .await;
        assert!(!d.scopes.contains(Scope::ReadMetadata));
    }

    /// Regression for the access_tags DIRECT-ACCESS bypass: an API key's
    /// `authn_access_tags` restriction must narrow the per-node decision to the
    /// intersection — the same narrowing `list_filter` applies — so a key scoped
    /// to `team-a` cannot reach a `team-b` node by direct path (it would 404),
    /// not merely be hidden from listings. Untagged/public nodes stay reachable
    /// under the narrow key, matching `list_filter`'s include_untagged + public.
    #[tokio::test]
    async fn principal_decision_honours_authn_access_tags_narrowing() {
        let db = setup_auth_db().await;
        // The principal is granted BOTH team-a and team-b in the DB.
        let p = principal_with_tags(&db, &["team-a", "team-b"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let meta = serde_json::json!({});
        let key_team_a: &[String] = &["team-a".to_string()];

        // team-b node + key restricted to [team-a]: DENIED. The key's restriction
        // overrides the principal's broader DB grant.
        let blob_b = serde_json::json!({"tags": ["team-b"]});
        let d_denied = policy
            .principal_decision(
                &principal_from(&p),
                &session,
                Some(key_team_a),
                ctx(&blob_b, &meta),
            )
            .await;
        assert!(
            !d_denied.scopes.contains(Scope::ReadMetadata),
            "key scoped to team-a must NOT reach a team-b node by direct access"
        );

        // Same principal, NO key restriction: the team-b grant applies.
        let d_unrestricted = policy
            .principal_decision(&principal_from(&p), &session, None, ctx(&blob_b, &meta))
            .await;
        assert!(
            d_unrestricted.scopes.contains(Scope::ReadMetadata),
            "without an authn_access_tags restriction the team-b grant applies"
        );

        // team-a node + [team-a] key: still granted.
        let blob_a = serde_json::json!({"tags": ["team-a"]});
        let d_a = policy
            .principal_decision(
                &principal_from(&p),
                &session,
                Some(key_team_a),
                ctx(&blob_a, &meta),
            )
            .await;
        assert!(
            d_a.scopes.contains(Scope::ReadMetadata),
            "key scoped to team-a reaches a team-a node"
        );

        // Untagged and public nodes remain reachable under the narrow key —
        // uniform with list_filter, which keeps include_untagged and the public
        // tag regardless of authn_access_tags.
        let blob_untagged = serde_json::json!({});
        let d_untagged = policy
            .principal_decision(
                &principal_from(&p),
                &session,
                Some(key_team_a),
                ctx(&blob_untagged, &meta),
            )
            .await;
        assert!(
            d_untagged.scopes.contains(Scope::ReadMetadata),
            "untagged nodes stay readable under a narrow key (matches include_untagged)"
        );
        let blob_public = serde_json::json!({"tags": ["public"]});
        let d_public = policy
            .principal_decision(
                &principal_from(&p),
                &session,
                Some(key_team_a),
                ctx(&blob_public, &meta),
            )
            .await;
        assert!(
            d_public.scopes.contains(Scope::ReadMetadata),
            "public nodes stay readable under a narrow key"
        );
    }

    /// End-to-end for the API-key write side (X4): a key created *with*
    /// `access_tags` persists that restriction, and reading it back drives
    /// `TagBasedPolicy` narrowing — a key scoped to `[team-a]` cannot reach a
    /// `team-b` node even though its principal holds both tags. This closes the
    /// loop the read-only side already had: create → persist → read → narrow.
    #[tokio::test]
    async fn api_key_access_tags_persist_and_narrow_through_policy() {
        let db = setup_auth_db().await;
        let p = principal_with_tags(&db, &["team-a", "team-b"]).await;

        // WRITE side: create a key restricted to [team-a].
        let material = db
            .create_api_key(crate::auth::ApiKeyCreate {
                principal_id: p.id,
                note: None,
                scopes: read_metadata(),
                expiration_time: None,
                access_tags: Some(vec!["team-a".to_string()]),
            })
            .await
            .expect("create tag-restricted key");

        // Read the restriction back from the DB — the same column the auth
        // middleware turns into `authn_access_tags`.
        let keys = db.list_api_keys(Some(p.id)).await.expect("list keys");
        let record = keys
            .iter()
            .find(|k| k.first_eight == material.record.first_eight)
            .expect("key present");
        assert_eq!(record.access_tags, vec!["team-a"], "access_tags persisted");

        // Drive TagBasedPolicy with the persisted restriction.
        let key_tags: Vec<String> = record.access_tags.clone();
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let meta = serde_json::json!({});

        // team-b node: principal holds team-b, but the key does not → DENIED.
        let blob_b = serde_json::json!({"tags": ["team-b"]});
        let d_b = policy
            .principal_decision(
                &principal_from(&p),
                &session,
                Some(&key_tags),
                ctx(&blob_b, &meta),
            )
            .await;
        assert!(
            !d_b.scopes.contains(Scope::ReadMetadata),
            "a key restricted to team-a must not reach a team-b node"
        );

        // team-a node → GRANTED.
        let blob_a = serde_json::json!({"tags": ["team-a"]});
        let d_a = policy
            .principal_decision(
                &principal_from(&p),
                &session,
                Some(&key_tags),
                ctx(&blob_a, &meta),
            )
            .await;
        assert!(
            d_a.scopes.contains(Scope::ReadMetadata),
            "a key restricted to team-a reaches a team-a node"
        );
    }

    /// Store-backed tag intersection: a principal sees only nodes whose
    /// `access_blob.tags` intersect the DB-stored principal tags.
    #[tokio::test]
    async fn store_backed_tag_intersection_filters_nodes() {
        let db = setup_auth_db().await;
        // alice has "team-a" in the DB; bob has "team-b".
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let bob = principal_with_tags(&db, &["team-b"]).await;
        let db_arc = Arc::new(db);
        let policy = TagBasedPolicy::new(db_arc.clone(), ScopeSet::full());
        let session = ScopeSet::for_role("user");

        let blob_team_a = serde_json::json!({"tags": ["team-a"]});
        let meta = serde_json::json!({});

        // Alice can see team-a nodes.
        let d_alice = policy
            .principal_decision(
                &principal_from(&alice),
                &session,
                None,
                ctx(&blob_team_a, &meta),
            )
            .await;
        assert!(
            d_alice.scopes.contains(Scope::ReadMetadata),
            "alice must see team-a nodes"
        );

        // Bob cannot see team-a nodes.
        let d_bob = policy
            .principal_decision(
                &principal_from(&bob),
                &session,
                None,
                ctx(&blob_team_a, &meta),
            )
            .await;
        assert!(
            !d_bob.scopes.contains(Scope::ReadMetadata),
            "bob must not see team-a nodes (only has team-b)"
        );

        // list_filter for alice must include "team-a"; for bob it must not.
        let fa = policy
            .list_filter(
                Some(&principal_from(&alice)),
                &session,
                &read_metadata(),
                None,
            )
            .await
            .unwrap();
        assert!(
            fa.tags.contains(&"team-a".to_string()),
            "alice filter has team-a"
        );
        assert!(
            !fa.tags.contains(&"team-b".to_string()),
            "alice filter has no team-b"
        );

        let fb = policy
            .list_filter(
                Some(&principal_from(&bob)),
                &session,
                &read_metadata(),
                None,
            )
            .await
            .unwrap();
        assert!(
            fb.tags.contains(&"team-b".to_string()),
            "bob filter has team-b"
        );
        assert!(
            !fb.tags.contains(&"team-a".to_string()),
            "bob filter has no team-a"
        );
    }

    // ---- TagBasedPolicy::init_node ----

    #[tokio::test]
    async fn tag_based_policy_init_node_unowned_tag_rejected() {
        let db = setup_auth_db().await;
        // Define both tags in the registry so the ownership check is reached.
        db.define_tag("team-a").await.unwrap();
        db.define_tag("team-b").await.unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["team-b"]});
        let result = policy
            .init_node(&principal_from(&alice), None, &session, Some(&blob))
            .await;
        assert!(
            result.is_err(),
            "non-admin assigning an unowned tag must be rejected"
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("is not an owner"),
            "error must name ownership failure, got: {msg}"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_init_node_owned_tag_accepted() {
        let db = setup_auth_db().await;
        db.define_tag("team-a").await.unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["team-a"]});
        let (modified, result) = policy
            .init_node(&principal_from(&alice), None, &session, Some(&blob))
            .await
            .expect("owned tag must be accepted");
        assert!(!modified, "blob unchanged when input == output");
        assert_eq!(
            result,
            serde_json::json!({"tags": ["team-a"]}),
            "result must mirror the input"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_init_node_admin_bypasses_ownership() {
        let db = setup_auth_db().await;
        // Admin bypasses OWNERSHIP but not is_tag_defined — tag must exist.
        db.define_tag("team-x").await.unwrap();
        let admin = admin_like("admin-uuid");
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::full(); // contains AdminApiKeys
        let blob = serde_json::json!({"tags": ["team-x"]});
        let (_, result) = policy
            .init_node(&admin, None, &session, Some(&blob))
            .await
            .expect("admin must bypass ownership check");
        assert_eq!(result, serde_json::json!({"tags": ["team-x"]}));
    }

    #[tokio::test]
    async fn tag_based_policy_init_node_undefined_tag_rejected_for_non_admin() {
        let db = setup_auth_db().await;
        // "new-tag" is NOT in the registry.
        let alice = principal_with_tags(&db, &["new-tag"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["new-tag"]});
        let result = policy
            .init_node(&principal_from(&alice), None, &session, Some(&blob))
            .await;
        assert!(
            result.is_err(),
            "undefined tag must be rejected for non-admin"
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("is not defined"),
            "error must name registry failure, got: {msg}"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_init_node_undefined_tag_rejected_for_admin() {
        let db = setup_auth_db().await;
        // "ghost-tag" is NOT in the registry. Even admin cannot bypass this.
        let admin = admin_like("admin-uuid");
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::full();
        let blob = serde_json::json!({"tags": ["ghost-tag"]});
        let result = policy.init_node(&admin, None, &session, Some(&blob)).await;
        assert!(
            result.is_err(),
            "undefined tag must be rejected even for admin"
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("is not defined"),
            "error must name registry failure, got: {msg}"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_init_node_no_blob_defaults_to_user_owned() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let (modified, result) = policy
            .init_node(&principal_from(&alice), None, &session, None)
            .await
            .expect("no-blob init must succeed");
        assert!(modified, "blob was generated (modified=true)");
        assert_eq!(
            result,
            serde_json::json!({"user": alice.uuid}),
            "no-blob init must default to user-owned blob"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_init_node_api_key_restriction_rejected() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a", "team-b"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["team-a"]});
        // API key only allows team-b, so team-a is outside the key scope
        let key_tags: Vec<String> = vec!["team-b".to_string()];
        let result = policy
            .init_node(
                &principal_from(&alice),
                Some(&key_tags),
                &session,
                Some(&blob),
            )
            .await;
        assert!(
            result.is_err(),
            "tag outside API key restriction must be rejected"
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("API key is restricted"),
            "error must mention key restriction, got: {msg}"
        );
    }

    // ---- TagBasedPolicy::modify_node ----

    #[tokio::test]
    async fn tag_based_policy_modify_node_unauthorized_retag_rejected() {
        let db = setup_auth_db().await;
        // Both tags must exist in registry so ownership check is reached.
        db.define_tag("team-a").await.unwrap();
        db.define_tag("team-b").await.unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let current = serde_json::json!({"tags": ["team-a"]});
        let proposed = serde_json::json!({"tags": ["team-b"]});
        let result = policy
            .modify_node(
                &current,
                &principal_from(&alice),
                None,
                &session,
                Some(&proposed),
            )
            .await;
        assert!(
            result.is_err(),
            "re-tagging to an unowned tag must be rejected"
        );
        let msg = result.unwrap_err();
        assert!(
            msg.contains("is not an owner"),
            "error must name ownership failure, got: {msg}"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_modify_node_removing_unowned_tag_rejected() {
        let db = setup_auth_db().await;
        // Both tags must exist in registry so ownership check is reached.
        db.define_tag("team-a").await.unwrap();
        db.define_tag("team-b").await.unwrap();
        // alice only owns team-a; node has team-a AND team-b
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let current = serde_json::json!({"tags": ["team-a", "team-b"]});
        // alice proposes to remove team-b (she doesn't own it)
        let proposed = serde_json::json!({"tags": ["team-a"]});
        let result = policy
            .modify_node(
                &current,
                &principal_from(&alice),
                None,
                &session,
                Some(&proposed),
            )
            .await;
        assert!(result.is_err(), "removing an unowned tag must be rejected");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("is not an owner"),
            "error must name ownership failure on removal, got: {msg}"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_modify_node_owner_preserving_change_allowed() {
        let db = setup_auth_db().await;
        db.define_tag("team-a").await.unwrap();
        db.define_tag("team-b").await.unwrap();
        let alice = principal_with_tags(&db, &["team-a", "team-b"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let current = serde_json::json!({"tags": ["team-a"]});
        let proposed = serde_json::json!({"tags": ["team-a", "team-b"]});
        // `modified` = "policy changed the proposed blob" (normalization), not
        // "blob changed from current". The proposed blob is returned as-is, so
        // modified = false — the server uses the returned blob to store.
        let (modified, result) = policy
            .modify_node(
                &current,
                &principal_from(&alice),
                None,
                &session,
                Some(&proposed),
            )
            .await
            .expect("adding an owned tag must be allowed");
        assert!(
            !modified,
            "proposed blob passed through unchanged by policy"
        );
        let out_tags = result["tags"].as_array().unwrap();
        assert!(
            out_tags.iter().any(|v| v == "team-a"),
            "team-a must be in result"
        );
        assert!(
            out_tags.iter().any(|v| v == "team-b"),
            "team-b must be in result"
        );
    }

    #[tokio::test]
    async fn tag_based_policy_modify_node_admin_bypasses_ownership() {
        let db = setup_auth_db().await;
        // Admin bypasses OWNERSHIP but not is_tag_defined — all tags must exist.
        db.define_tag("team-a").await.unwrap();
        db.define_tag("team-x").await.unwrap();
        let admin = admin_like("admin-uuid");
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::full();
        let current = serde_json::json!({"tags": ["team-a"]});
        let proposed = serde_json::json!({"tags": ["team-x"]});
        let (_, result) = policy
            .modify_node(&current, &admin, None, &session, Some(&proposed))
            .await
            .expect("admin must bypass ownership check on modify");
        assert_eq!(result, serde_json::json!({"tags": ["team-x"]}));
    }

    #[tokio::test]
    async fn tag_based_policy_modify_node_no_op_returns_unchanged() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let current = serde_json::json!({"tags": ["team-a"]});
        let (modified, result) = policy
            .modify_node(
                &current,
                &principal_from(&alice),
                None,
                &session,
                Some(&current),
            )
            .await
            .expect("no-op modify must succeed");
        assert!(!modified, "identical blob must not signal modification");
        assert_eq!(result, current);
    }

    /// An API key with a narrower `access_tags` restriction produces a smaller
    /// effective tag set than the principal alone, so it sees fewer tagged nodes.
    /// alice has [team-a, team-b] in the DB.
    /// Key 1: no tag restriction (empty access_tags) → sees both tags.
    /// Key 2: access_tags = [team-a]                → intersection = [team-a] only.
    /// Key 3: access_tags = [team-c] (no overlap)   → intersection = [] (nothing
    ///   tagged). Only untagged / public nodes would show.
    #[tokio::test]
    async fn apikey_with_narrower_access_tags_sees_fewer_nodes_than_its_principal() {
        let db = setup_auth_db().await;
        let alice = principal_with_tags(&db, &["team-a", "team-b"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");

        // Key 1: unrestricted (no authn_access_tags).
        let f_full = policy
            .list_filter(
                Some(&principal_from(&alice)),
                &session,
                &read_metadata(),
                None,
            )
            .await
            .unwrap();
        assert!(
            f_full.tags.contains(&"team-a".to_string()),
            "unrestricted key sees team-a"
        );
        assert!(
            f_full.tags.contains(&"team-b".to_string()),
            "unrestricted key sees team-b"
        );

        // Key 2: restricted to [team-a].
        let key_tags_a: Vec<String> = vec!["team-a".to_string()];
        let f_narrow = policy
            .list_filter(
                Some(&principal_from(&alice)),
                &session,
                &read_metadata(),
                Some(&key_tags_a),
            )
            .await
            .unwrap();
        assert!(
            f_narrow.tags.contains(&"team-a".to_string()),
            "narrowed key sees team-a"
        );
        assert!(
            !f_narrow.tags.contains(&"team-b".to_string()),
            "narrowed key must NOT see team-b (out of intersection)"
        );

        // Key 3: restricted to [team-c] — no overlap with principal's [team-a, team-b].
        let key_tags_c: Vec<String> = vec!["team-c".to_string()];
        let f_empty = policy
            .list_filter(
                Some(&principal_from(&alice)),
                &session,
                &read_metadata(),
                Some(&key_tags_c),
            )
            .await
            .unwrap();
        // Intersection is empty, so only untagged + public tags appear.
        assert!(
            !f_empty.tags.contains(&"team-a".to_string()),
            "disjoint key sees no team-a"
        );
        assert!(
            !f_empty.tags.contains(&"team-b".to_string()),
            "disjoint key sees no team-b"
        );
    }

    // ---- per-tag scope resolution (commit c) ----

    /// A tag configured with only read:metadata must NOT grant write:metadata
    /// in principal_decision. This verifies per-tag scope resolution replaces
    /// the uniform default_scopes behaviour.
    #[tokio::test]
    async fn per_tag_scope_read_only_tag_does_not_grant_write() {
        let db = setup_auth_db().await;
        db.seed_tag("team-a", &["read:metadata".to_string()])
            .await
            .unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        // Policy has full default_scopes, but tag_scopes limits team-a to read:metadata.
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        // Use user (not admin) session to avoid admin short-circuit.
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["team-a"]});
        let meta = serde_json::json!({});
        let decision = policy
            .principal_decision(&principal_from(&alice), &session, None, ctx(&blob, &meta))
            .await;
        assert!(
            decision.scopes.contains(Scope::ReadMetadata),
            "read:metadata must be granted"
        );
        assert!(
            !decision.scopes.contains(Scope::WriteMetadata),
            "write:metadata must NOT be granted when tag only has read:metadata"
        );
    }

    /// A tag configured with only read:metadata must be EXCLUDED from
    /// list_filter when write:metadata is requested.
    #[tokio::test]
    async fn per_tag_scope_list_filter_excludes_tag_that_does_not_grant_write() {
        let db = setup_auth_db().await;
        db.seed_tag("team-a", &["read:metadata".to_string()])
            .await
            .unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        // Policy default_scopes = full so NO_ACCESS guard passes for write:metadata.
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        // Use user (not admin) session to avoid admin ALL_ACCESS short-circuit.
        let session = ScopeSet::for_role("user");
        let write_meta = ScopeSet::from_iter([Scope::WriteMetadata]);
        let f = policy
            .list_filter(Some(&principal_from(&alice)), &session, &write_meta, None)
            .await
            .unwrap();
        assert!(
            !f.tags.contains(&"team-a".to_string()),
            "team-a only grants read:metadata so it must not appear in a write:metadata filter"
        );
    }

    /// Tags with NO tag_scopes rows must fall back to default_scopes.
    /// Backward-compat: existing deployments without tag_scopes data must
    /// continue to work as before migration 0005.
    #[tokio::test]
    async fn backward_compat_empty_tag_scopes_falls_back_to_default() {
        let db = setup_auth_db().await;
        // define_tag only — no set_tag_scopes call, so tag_scopes has no rows.
        db.define_tag("team-a").await.unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        // Policy default_scopes = full (write-capable).
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        // Use user (not admin) session to avoid admin short-circuits.
        let session = ScopeSet::for_role("user");

        // principal_decision: empty tag_scopes → fallback to default_scopes=full
        let blob = serde_json::json!({"tags": ["team-a"]});
        let meta = serde_json::json!({});
        let decision = policy
            .principal_decision(&principal_from(&alice), &session, None, ctx(&blob, &meta))
            .await;
        assert!(
            decision.scopes.contains(Scope::WriteMetadata),
            "write:metadata must be granted via default_scopes fallback when tag_scopes is empty"
        );

        // list_filter: empty tag_scopes → fallback to default_scopes=full →
        // tag passes any write-scope requested_scopes filter.
        let write_meta = ScopeSet::from_iter([Scope::WriteMetadata]);
        let f = policy
            .list_filter(Some(&principal_from(&alice)), &session, &write_meta, None)
            .await
            .unwrap();
        assert!(
            f.tags.contains(&"team-a".to_string()),
            "team-a with empty tag_scopes must appear in write:metadata filter via default_scopes fallback"
        );
    }

    // ---- unremovable_scopes (commit d) ----

    /// Non-admin retagging a node to a read-only-only tag configuration must be
    /// rejected: write:metadata would be lost (self-lockout guard).
    /// Mirrors access_policies.py unremovable_scopes check.
    #[tokio::test]
    async fn unremovable_scopes_blocks_self_lockout_modify() {
        let db = setup_auth_db().await;
        // team-a: read-only. team-b: full scopes (via default fallback).
        db.seed_tag("team-a", &["read:metadata".to_string()])
            .await
            .unwrap();
        db.define_tag("team-b").await.unwrap(); // no tag_scopes → fallback to default=full
        let alice = principal_with_tags(&db, &["team-a", "team-b"]).await;
        // Policy default_scopes=full so team-b fallback includes write:metadata.
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");

        // Node currently tagged with both. Alice proposes to drop team-b,
        // leaving only team-a (read:metadata). This removes write:metadata → lockout.
        let current = serde_json::json!({"tags": ["team-a", "team-b"]});
        let proposed = serde_json::json!({"tags": ["team-a"]});
        let result = policy
            .modify_node(
                &current,
                &principal_from(&alice),
                None,
                &session,
                Some(&proposed),
            )
            .await;
        assert!(result.is_err(), "lockout retag must be rejected");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("required scope"),
            "error must name unremovable scope violation, got: {msg}"
        );
    }

    /// Non-admin creating a node with only a read-only tag is also rejected
    /// by unremovable_scopes (init_node path).
    #[tokio::test]
    async fn unremovable_scopes_blocks_read_only_init() {
        let db = setup_auth_db().await;
        db.seed_tag("team-a", &["read:metadata".to_string()])
            .await
            .unwrap();
        let alice = principal_with_tags(&db, &["team-a"]).await;
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::for_role("user");
        let blob = serde_json::json!({"tags": ["team-a"]});
        let result = policy
            .init_node(&principal_from(&alice), None, &session, Some(&blob))
            .await;
        assert!(result.is_err(), "read-only-tag init must be rejected");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("required scope"),
            "error must name unremovable scope violation, got: {msg}"
        );
    }

    /// Admin is exempt from the unremovable_scopes check — they can assign
    /// any tag combination, including read-only-only.
    #[tokio::test]
    async fn unremovable_scopes_admin_exempt() {
        let db = setup_auth_db().await;
        db.seed_tag("team-a", &["read:metadata".to_string()])
            .await
            .unwrap();
        let admin = admin_like("admin-uuid");
        let policy = TagBasedPolicy::new(Arc::new(db), ScopeSet::full());
        let session = ScopeSet::full();
        let blob = serde_json::json!({"tags": ["team-a"]});
        let (_, result) = policy
            .init_node(&admin, None, &session, Some(&blob))
            .await
            .expect("admin must bypass unremovable_scopes check");
        assert_eq!(result, serde_json::json!({"tags": ["team-a"]}));
    }
}
