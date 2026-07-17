//! Scope strings — granular permissions enforced per HTTP route.
//!
//! Mirrors `tiled.scopes` (`tiled/scopes.py`). The set is closed: anything
//! not in [`Scope::ALL`] cannot be granted, so a typo in a CLI command or
//! config doesn't accidentally hand out a wildcard.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    ReadMetadata,
    ReadData,
    WriteMetadata,
    WriteData,
    CreateNode,
    Register,
    DeleteNode,
    DeleteRevision,
    CreateApiKeys,
    RevokeApiKeys,
    AdminApiKeys,
    ReadPrincipals,
    WritePrincipals,
    Inherit,
    Metrics,
    Admin,
    /// Read webhooks registered on a node + their delivery history.
    /// Mirrors upstream tiled PR #1353. Granted to admin only by default.
    ReadWebhooks,
    /// Register / deactivate webhooks. Admin only by default.
    WriteWebhooks,
}

impl Scope {
    pub const ALL: &'static [Scope] = &[
        Self::ReadMetadata,
        Self::ReadData,
        Self::WriteMetadata,
        Self::WriteData,
        Self::CreateNode,
        Self::Register,
        Self::DeleteNode,
        Self::DeleteRevision,
        Self::CreateApiKeys,
        Self::RevokeApiKeys,
        Self::AdminApiKeys,
        Self::ReadPrincipals,
        Self::WritePrincipals,
        Self::Inherit,
        Self::Metrics,
        Self::Admin,
        Self::ReadWebhooks,
        Self::WriteWebhooks,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadMetadata => "read:metadata",
            Self::ReadData => "read:data",
            Self::WriteMetadata => "write:metadata",
            Self::WriteData => "write:data",
            Self::CreateNode => "create:node",
            Self::Register => "register",
            Self::DeleteNode => "delete:node",
            Self::DeleteRevision => "delete:revision",
            Self::CreateApiKeys => "create:apikeys",
            Self::RevokeApiKeys => "revoke:apikeys",
            Self::AdminApiKeys => "admin:apikeys",
            Self::ReadPrincipals => "read:principals",
            Self::WritePrincipals => "write:principals",
            Self::Inherit => "inherit",
            Self::Metrics => "metrics",
            Self::Admin => "admin",
            Self::ReadWebhooks => "read:webhooks",
            Self::WriteWebhooks => "write:webhooks",
        }
    }

    pub fn parse(s: &str) -> Option<Scope> {
        Self::ALL.iter().copied().find(|sc| sc.as_str() == s)
    }
}

impl Serialize for Scope {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        ser.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Scope {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        let s = String::deserialize(de)?;
        Scope::parse(&s)
            .ok_or_else(|| <D::Error as serde::de::Error>::custom(format!("unknown scope: {s}")))
    }
}

/// A set of scopes attached to a principal / api-key / session. Stored as
/// JSON arrays of strings on disk; held as `BTreeSet<Scope>` in memory so
/// `contains` is O(log n) and ordering is stable.
///
/// The `admin` superscope implies every scope. To keep that implication
/// from having two meanings (honored on the `contains` path but ignored by
/// the literal `intersect`/subset cap primitive), the inner set is
/// **canonicalized on construction**: any set that contains [`Scope::Admin`]
/// is materialized to the full set. The invariant `Admin ∈ set ⟹ set ==
/// full()` therefore holds *by construction*, so `contains`, `intersect`,
/// and the JSON/subset paths all agree — there is no bare `{Admin}` that
/// could mean "all" on one operation and "just admin" on another. The inner
/// field is private so no caller can bypass canonicalization.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ScopeSet(BTreeSet<Scope>);

impl ScopeSet {
    /// Materialize the `admin ⟹ all` implication: a set that contains
    /// [`Scope::Admin`] becomes the full set, so no constructed `ScopeSet`
    /// ever encodes "all scopes" as a bare `{Admin}`.
    fn canonicalize(set: BTreeSet<Scope>) -> BTreeSet<Scope> {
        if set.contains(&Scope::Admin) {
            Scope::ALL.iter().copied().collect()
        } else {
            set
        }
    }

    pub fn new() -> Self {
        Self(BTreeSet::new())
    }

    pub fn full() -> Self {
        Self(Scope::ALL.iter().copied().collect())
    }

    pub fn read_only() -> Self {
        Self([Scope::ReadMetadata, Scope::ReadData].into_iter().collect())
    }

    /// Scopes granted to the single-user API key. Mirrors upstream
    /// `SINGLE_USER_SCOPES` exactly (`tiled/access_control/scopes.py:32-46`):
    /// the node-I/O scopes plus `metrics` and the webhook scopes, but
    /// **deliberately excluding** the credential/principal-management scopes
    /// (`create:apikeys`, `revoke:apikeys`, `admin:apikeys`, `read:principals`,
    /// `write:principals`) and the `admin` superscope — single-user mode has no
    /// principal/API-key database to manage. Upstream grants this set (not the
    /// full set) in the single-user branch of `get_scopes_from_api_key`
    /// (`tiled/server/authentication.py:352-356`); granting `full()` here would
    /// hand the single-user key `admin`, which — in a mixed-mode
    /// misconfiguration — is a privilege-escalation backdoor.
    pub fn single_user() -> Self {
        Self::from_iter([
            Scope::ReadMetadata,
            Scope::ReadData,
            Scope::WriteMetadata,
            Scope::WriteData,
            Scope::DeleteRevision,
            Scope::DeleteNode,
            Scope::CreateNode,
            Scope::Register,
            Scope::Metrics,
            Scope::ReadWebhooks,
            Scope::WriteWebhooks,
        ])
    }

    /// Scopes granted to principals with the named role. Mirrors Python's
    /// `create_default_roles` in `tiled/authn_database/core.py`.
    ///
    /// * `"user"` — all node I/O scopes plus `create:apikeys`/`revoke:apikeys`.
    /// * `"admin"` — all scopes (equivalent to `full()`).
    /// * any other string — empty (deny-by-default; log a warning at call site).
    pub fn for_role(role: &str) -> Self {
        match role {
            "user" => Self::from_iter([
                Scope::ReadMetadata,
                Scope::ReadData,
                Scope::CreateNode,
                Scope::WriteMetadata,
                Scope::WriteData,
                Scope::DeleteRevision,
                Scope::DeleteNode,
                Scope::CreateApiKeys,
                Scope::RevokeApiKeys,
            ]),
            "admin" => Self::full(),
            _ => Self::new(),
        }
    }

    /// Parse from JSON-array text (`'["read:metadata", "read:data"]'`).
    /// Unknown scope names produce `Validation` errors so we never silently
    /// drop a misspelled scope from a CLI invocation.
    pub fn from_json(s: &str) -> crate::auth::error::Result<Self> {
        let arr: Vec<String> = serde_json::from_str(s)?;
        let mut set = BTreeSet::new();
        for name in arr {
            let scope = Scope::parse(&name).ok_or_else(|| {
                crate::auth::error::AuthError::Validation(format!("unknown scope: {name}"))
            })?;
            set.insert(scope);
        }
        Ok(Self(Self::canonicalize(set)))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.0.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            .unwrap_or_else(|_| "[]".into())
    }

    pub fn insert(&mut self, scope: Scope) -> bool {
        let added = self.0.insert(scope);
        if scope == Scope::Admin {
            // Maintain the canonical invariant: granting `admin` confers all.
            self.0 = Scope::ALL.iter().copied().collect();
        }
        added
    }

    /// True iff `scope` is granted. No `admin` special-case is needed: a set
    /// that holds [`Scope::Admin`] was canonicalized to the full set at
    /// construction, so plain membership already honors the implication.
    pub fn contains(&self, scope: Scope) -> bool {
        self.0.contains(&scope)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = Scope> + '_ {
        self.0.iter().copied()
    }

    /// Set intersection used as the scope cap/subset primitive. Because no
    /// operand can be a bare `{Admin}` (canonicalized to full on
    /// construction), this literal intersection agrees with [`Self::contains`].
    pub fn intersect(&self, other: &Self) -> Self {
        Self(self.0.intersection(&other.0).copied().collect())
    }

    /// Set union. Used to combine token-derived scopes with a principal's
    /// role scopes for external OIDC sessions (Python `get_current_scopes`
    /// returns `token_scopes | role_scopes`, `authentication.py:434`). The
    /// result is canonicalized, so unioning in `admin` still yields `full()`.
    pub fn union(&self, other: &Self) -> Self {
        Self(Self::canonicalize(
            self.0.union(&other.0).copied().collect(),
        ))
    }

    /// Subset test: every scope in `self` is also in `other`. The access
    /// policy uses it for the NO_ACCESS gate (requested scopes must be
    /// grantable) and the read-only check (untagged/public rows only for
    /// read-scoped listings). Like [`Self::intersect`], canonicalization
    /// (no operand is a bare `{Admin}`) keeps this in agreement with
    /// [`Self::contains`].
    pub fn is_subset(&self, other: &Self) -> bool {
        self.0.is_subset(&other.0)
    }

    /// Expand the [`Scope::Inherit`] metascope. `inherit` is not a real
    /// permission: it means "dynamically receive the principal's *current*
    /// role scopes at access time" (Python parity: `inherit` confers all
    /// principal scopes, `authentication.py:372-381`). When `self` contains
    /// `Inherit`, it is replaced by `role_scopes`; otherwise `self` is
    /// returned unchanged. The result is canonicalized like any other
    /// `ScopeSet`. This is the single owner of `inherit` meaning — without
    /// it `Inherit` grants nothing, leaving a dead permission.
    pub fn expand_inherit(&self, role_scopes: &Self) -> Self {
        if !self.0.contains(&Scope::Inherit) {
            return self.clone();
        }
        let mut out = self.0.clone();
        out.remove(&Scope::Inherit);
        out.extend(role_scopes.0.iter().copied());
        Self(Self::canonicalize(out))
    }
}

impl FromIterator<Scope> for ScopeSet {
    fn from_iter<I: IntoIterator<Item = Scope>>(iter: I) -> Self {
        Self(Self::canonicalize(iter.into_iter().collect()))
    }
}

impl<'de> Deserialize<'de> for ScopeSet {
    fn deserialize<D: serde::Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        // On-disk / wire form is a JSON array of scope strings. Canonicalize
        // on load so a stored bare `["admin"]` becomes the full set, keeping
        // the `Admin ⟹ all` invariant true at the store/load boundary.
        let set = BTreeSet::<Scope>::deserialize(de)?;
        Ok(Self(Self::canonicalize(set)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_json() {
        let s = ScopeSet::from_iter([Scope::ReadMetadata, Scope::WriteMetadata]);
        let raw = s.to_json();
        let parsed = ScopeSet::from_json(&raw).unwrap();
        assert_eq!(s, parsed);
    }

    #[test]
    fn admin_implies_everything() {
        let s = ScopeSet::from_iter([Scope::Admin]);
        assert!(s.contains(Scope::DeleteNode));
        assert!(s.contains(Scope::CreateApiKeys));
        assert!(s.contains(Scope::AdminApiKeys));
    }

    /// Finding 3: the `admin` superscope must mean the same thing on the
    /// `intersect`/subset cap path as on `contains`. Because a set holding
    /// `Admin` is canonicalized to `full()` on construction, intersecting it
    /// with X is identical to intersecting `full()` with X (i.e. yields X).
    #[test]
    fn admin_set_intersect_matches_full_intersect() {
        let admin = ScopeSet::from_iter([Scope::Admin]);
        let full = ScopeSet::full();
        // Canonicalization: a bare `{Admin}` is materialized to the full set.
        assert_eq!(admin, full, "a set containing Admin normalizes to full()");

        let x = ScopeSet::from_iter([Scope::ReadData, Scope::WriteData]);
        assert_eq!(
            admin.intersect(&x),
            full.intersect(&x),
            "intersect must treat Admin the same as full()"
        );
        // full ∩ x == x — capping by an admin set never drops a real scope.
        assert_eq!(admin.intersect(&x), x);
    }

    /// #1360: `union` combines token-derived scopes with role scopes for
    /// external OIDC sessions, and stays canonical (unioning in `admin`
    /// yields `full()`).
    #[test]
    fn union_combines_and_canonicalizes() {
        let token = ScopeSet::from_iter([Scope::ReadMetadata, Scope::ReadData]);
        let role = ScopeSet::from_iter([Scope::WriteData]);
        let combined = token.union(&role);
        assert!(combined.contains(Scope::ReadMetadata));
        assert!(combined.contains(Scope::ReadData));
        assert!(combined.contains(Scope::WriteData));

        // admin on either side collapses to the full set.
        let with_admin = token.union(&ScopeSet::from_iter([Scope::Admin]));
        assert_eq!(with_admin, ScopeSet::full());

        // union with empty is identity.
        assert_eq!(token.union(&ScopeSet::new()), token);
    }

    /// Finding 1: the `inherit` metascope must expand to the principal's
    /// current role scopes (Python dynamic inheritance), not stay a dead
    /// permission. This mirrors the composition `resolve_api_key_scopes`
    /// performs: `expand_inherit(role)` then cap by `role ∩ default_login`.
    #[test]
    fn inherit_expands_to_current_role_scopes() {
        let role = ScopeSet::for_role("user");
        let default_login = ScopeSet::full();
        let cap = role.intersect(&default_login);

        // A key granted only `inherit` resolves to the current role scopes.
        let inherit_key = ScopeSet::from_iter([Scope::Inherit]);
        let resolved = inherit_key.expand_inherit(&role).intersect(&cap);
        assert_eq!(resolved, role, "inherit resolves to the role's scopes");
        assert!(
            resolved.contains(Scope::ReadData),
            "an inherit key is not a dead, permission-less credential"
        );
        assert!(
            !resolved.contains(Scope::Inherit),
            "the metascope is dropped — it is not a real permission"
        );

        // A non-inherit key is unchanged by expansion.
        let plain = ScopeSet::from_iter([Scope::ReadData]);
        assert_eq!(plain.expand_inherit(&role), plain);

        // Role downgrade takes effect: the same inherit key resolves to the
        // narrower set when the role narrows. user lacks Register; admin has it.
        let admin_resolved = inherit_key
            .expand_inherit(&ScopeSet::for_role("admin"))
            .intersect(&ScopeSet::for_role("admin").intersect(&default_login));
        assert!(admin_resolved.contains(Scope::Register));
        assert!(!resolved.contains(Scope::Register));
    }

    /// Canonicalization holds at every construction boundary, including the
    /// JSON/serde load path used for sessions stored as `["admin"]`.
    #[test]
    fn admin_canonicalizes_on_load() {
        let from_text = ScopeSet::from_json("[\"admin\"]").unwrap();
        assert_eq!(from_text, ScopeSet::full());

        let from_serde: ScopeSet = serde_json::from_str("[\"admin\"]").unwrap();
        assert_eq!(from_serde, ScopeSet::full());

        let mut grown = ScopeSet::read_only();
        grown.insert(Scope::Admin);
        assert_eq!(grown, ScopeSet::full());
    }

    #[test]
    fn unknown_scope_rejected() {
        let err = ScopeSet::from_json("[\"foo\"]").unwrap_err();
        assert!(matches!(err, crate::auth::error::AuthError::Validation(_)));
    }

    /// Every canonical string from Python tiled scopes.py must parse and
    /// round-trip through as_str without loss.
    #[test]
    fn canonical_python_scope_strings_parse_and_roundtrip() {
        let canonical = [
            "read:metadata",
            "read:data",
            "write:metadata",
            "write:data",
            "delete:revision",
            "delete:node",
            "create:node",
            "register",
            "metrics",
            "create:apikeys",
            "revoke:apikeys",
            "admin:apikeys",
            "read:principals",
            "write:principals",
            "read:webhooks",
            "write:webhooks",
        ];
        for s in canonical {
            let scope = Scope::parse(s).unwrap_or_else(|| panic!("failed to parse: {s}"));
            assert_eq!(scope.as_str(), s, "as_str mismatch for {s}");
        }
    }

    /// `single_user()` must mirror upstream `SINGLE_USER_SCOPES`
    /// (`tiled/access_control/scopes.py:32-46`) exactly: the 11 node-I/O +
    /// metrics + webhook scopes, and NONE of the credential/principal
    /// management scopes or the `admin` superscope. This is the set the
    /// single-user key is granted instead of `full()`.
    #[test]
    fn single_user_matches_upstream_single_user_scopes() {
        let s = ScopeSet::single_user();
        let expected = [
            Scope::ReadMetadata,
            Scope::ReadData,
            Scope::WriteMetadata,
            Scope::WriteData,
            Scope::DeleteRevision,
            Scope::DeleteNode,
            Scope::CreateNode,
            Scope::Register,
            Scope::Metrics,
            Scope::ReadWebhooks,
            Scope::WriteWebhooks,
        ];
        for sc in expected {
            assert!(s.contains(sc), "single_user() must grant {}", sc.as_str());
        }
        // Exactly the 11 upstream scopes — nothing extra crept in.
        assert_eq!(s.iter().count(), expected.len());
        // The credential/principal-management scopes and `admin` superscope
        // upstream excludes — the whole point of not using full().
        for sc in [
            Scope::CreateApiKeys,
            Scope::RevokeApiKeys,
            Scope::AdminApiKeys,
            Scope::ReadPrincipals,
            Scope::WritePrincipals,
            Scope::Admin,
            Scope::Inherit,
        ] {
            assert!(
                !s.contains(sc),
                "single_user() must NOT grant {}",
                sc.as_str()
            );
        }
    }
}
