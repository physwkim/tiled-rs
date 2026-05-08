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
    Create,
    Register, // alias the Python world keeps for "register a new node"
    DeleteNode,
    DeleteRevision,
    ApiKeyCreate,
    ApiKeyRevoke,
    Inherit,
    Metrics,
    Admin,
}

impl Scope {
    pub const ALL: &'static [Scope] = &[
        Self::ReadMetadata,
        Self::ReadData,
        Self::WriteMetadata,
        Self::WriteData,
        Self::Create,
        Self::Register,
        Self::DeleteNode,
        Self::DeleteRevision,
        Self::ApiKeyCreate,
        Self::ApiKeyRevoke,
        Self::Inherit,
        Self::Metrics,
        Self::Admin,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::ReadMetadata => "read:metadata",
            Self::ReadData => "read:data",
            Self::WriteMetadata => "write:metadata",
            Self::WriteData => "write:data",
            Self::Create => "create",
            Self::Register => "register",
            Self::DeleteNode => "delete:node",
            Self::DeleteRevision => "delete:revision",
            Self::ApiKeyCreate => "apikeys:create",
            Self::ApiKeyRevoke => "apikeys:revoke",
            Self::Inherit => "inherit",
            Self::Metrics => "metrics",
            Self::Admin => "admin",
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
        Scope::parse(&s).ok_or_else(|| {
            <D::Error as serde::de::Error>::custom(format!("unknown scope: {s}"))
        })
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
        Self(
            [Scope::ReadMetadata, Scope::ReadData]
                .into_iter()
                .collect(),
        )
    }

    pub fn from_iter<I: IntoIterator<Item = Scope>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
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
        assert!(s.contains(Scope::ApiKeyCreate));
    }

    #[test]
    fn unknown_scope_rejected() {
        let err = ScopeSet::from_json("[\"foo\"]").unwrap_err();
        assert!(matches!(err, crate::error::AuthError::Validation(_)));
    }
}
