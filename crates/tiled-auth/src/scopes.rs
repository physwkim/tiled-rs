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
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeSet(pub BTreeSet<Scope>);

impl ScopeSet {
    pub fn new() -> Self {
        Self(BTreeSet::new())
    }

    pub fn full() -> Self {
        Self(Scope::ALL.iter().copied().collect())
    }

    pub fn read_only() -> Self {
        Self([Scope::ReadMetadata, Scope::ReadData].into_iter().collect())
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
    pub fn from_json(s: &str) -> crate::error::Result<Self> {
        let arr: Vec<String> = serde_json::from_str(s)?;
        let mut set = BTreeSet::new();
        for name in arr {
            let scope = Scope::parse(&name).ok_or_else(|| {
                crate::error::AuthError::Validation(format!("unknown scope: {name}"))
            })?;
            set.insert(scope);
        }
        Ok(Self(set))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self.0.iter().map(|s| s.as_str()).collect::<Vec<_>>())
            .unwrap_or_else(|_| "[]".into())
    }

    pub fn insert(&mut self, scope: Scope) -> bool {
        self.0.insert(scope)
    }

    pub fn contains(&self, scope: Scope) -> bool {
        self.0.contains(&scope) || self.0.contains(&Scope::Admin)
    }

    pub fn iter(&self) -> impl Iterator<Item = Scope> + '_ {
        self.0.iter().copied()
    }

    pub fn intersect(&self, other: &Self) -> Self {
        Self(self.0.intersection(&other.0).copied().collect())
    }
}

impl FromIterator<Scope> for ScopeSet {
    fn from_iter<I: IntoIterator<Item = Scope>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
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

    #[test]
    fn unknown_scope_rejected() {
        let err = ScopeSet::from_json("[\"foo\"]").unwrap_err();
        assert!(matches!(err, crate::error::AuthError::Validation(_)));
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
}
