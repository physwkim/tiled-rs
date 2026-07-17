//! Spec validation registry (upstream `tiled/validation_registration.py` plus
//! the server-side `validate_specs` helper, `server/router.py:2729-2769`).
//!
//! A node's `specs` name the specifications it conforms to. Operators may
//! register a validator per spec that inspects the node's (proposed) metadata —
//! and, on create, its structure — and either accepts it, rejects it, or
//! returns a normalized metadata document. The registry is consulted on node
//! create and on metadata update; the `reject_undeclared_specs` toggle (default
//! off) additionally turns a spec with no registered validator into a hard
//! rejection.
//!
//! Deviation from upstream: upstream populates the registry from import-path
//! callables named in YAML config (`validation_registry`, dispatched via
//! `import_object`). Rust has no import-path-callable analogue, so the registry
//! is populated programmatically (in-process) instead. The wire-expressible
//! half — the `reject_undeclared_specs` boolean — IS wired from YAML config.
//! The shipped `composite` validator is out of scope (the plugin substrate),
//! so the default registry is empty; the MECHANISM is what is ported here.

use std::collections::HashMap;
use std::sync::Arc;

use crate::core::structures::{Spec, StructureFamily};
use crate::server::error::ServerError;

/// Rejection raised by a spec validator. The message is surfaced to the client
/// as part of the HTTP 400 `failed validation for the <spec> spec:\n<message>`
/// detail (upstream `ValidationError`).
#[derive(Debug, Clone)]
pub struct ValidationError(pub String);

impl ValidationError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

/// What the node being validated looks like — the Rust analogue of upstream's
/// `(entry, structure_family, structure)` positional args, with the
/// create-vs-update distinction made explicit rather than inferred from which
/// argument is `None`.
#[derive(Clone, Copy)]
pub enum ValidationTarget<'a> {
    /// Creating a new node — the entry does not exist yet. Carries the declared
    /// structure family and (for non-container families) the structure payload,
    /// matching upstream's `entry=None, structure_family=..., structure=...`.
    Create {
        structure_family: StructureFamily,
        structure: Option<&'a serde_json::Value>,
    },
    /// Updating an existing node — carries the stored node's structure family,
    /// matching upstream's `entry=<adapter>` (from which a validator reads
    /// `entry.structure_family`).
    Update { structure_family: StructureFamily },
}

/// Context handed to a validator: the node's (proposed) metadata plus the
/// create/update target.
pub struct ValidationContext<'a> {
    pub metadata: &'a serde_json::Value,
    pub target: ValidationTarget<'a>,
}

/// A registered validator. Returns `Ok(None)` to accept the metadata unchanged,
/// `Ok(Some(_))` with a normalized metadata document to accept-and-rewrite, or
/// `Err(_)` to reject. Synchronous (not async) because every Rust-expressible
/// validator is a pure function of the context — upstream's only async
/// validator (`composite`, which awaits `entry.items_range()`) is out of scope.
pub type Validator = Arc<
    dyn Fn(&Spec, &ValidationContext) -> Result<Option<serde_json::Value>, ValidationError>
        + Send
        + Sync,
>;

/// Registry mapping a `Spec` (name + optional version) to its validator
/// (upstream `ValidationRegistry`). Keyed by the full `Spec`, so a registered
/// `composite@None` does not match a node's `composite@1.0` — matching upstream,
/// where lookup is by the frozen `Spec` dataclass.
#[derive(Clone, Default)]
pub struct ValidationRegistry {
    lookup: HashMap<Spec, Validator>,
}

impl ValidationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `func` for `spec`, replacing any existing entry (upstream
    /// `register`).
    pub fn register(&mut self, spec: Spec, func: Validator) {
        self.lookup.insert(spec, func);
    }

    /// Whether a validator is registered for `spec` (upstream `__contains__`).
    pub fn contains(&self, spec: &Spec) -> bool {
        self.lookup.contains_key(spec)
    }

    /// The validator for `spec`, if any (upstream `dispatch`).
    pub fn get(&self, spec: &Spec) -> Option<&Validator> {
        self.lookup.get(spec)
    }
}

/// The server-side validation policy carried on `AppState`: the registry plus
/// the `reject_undeclared_specs` toggle. Default = empty registry, permissive
/// (reject off), matching upstream's defaults so nothing changes unless an
/// operator opts in.
#[derive(Clone, Default)]
pub struct ValidationConfig {
    pub registry: ValidationRegistry,
    pub reject_undeclared_specs: bool,
}

/// Validate `specs` against `registry`, mirroring upstream `validate_specs`
/// (`router.py:2729-2769`). Returns `(metadata_modified, metadata)` on success;
/// on failure returns a `ServerError::BadRequest` carrying upstream's exact
/// detail shape (HTTP 400).
///
/// Specs are validated in REVERSE order (least-constrained first) so a lenient
/// spec's normalization can help a stricter one pass — matching upstream's
/// `for spec in reversed(specs)`. A spec with no registered validator is
/// skipped unless `reject_undeclared_specs`, in which case it is rejected.
pub fn validate_specs(
    registry: &ValidationRegistry,
    reject_undeclared_specs: bool,
    specs: &[Spec],
    mut metadata: serde_json::Value,
    target: ValidationTarget<'_>,
) -> Result<(bool, serde_json::Value), ServerError> {
    let mut modified = false;
    for spec in specs.iter().rev() {
        match registry.get(spec) {
            None => {
                if reject_undeclared_specs {
                    return Err(ServerError::BadRequest(format!(
                        "Unrecognized spec: {}",
                        spec.name
                    )));
                }
            }
            Some(validator) => {
                let ctx = ValidationContext {
                    metadata: &metadata,
                    target,
                };
                match validator(spec, &ctx) {
                    Ok(Some(new_metadata)) => {
                        metadata = new_metadata;
                        modified = true;
                    }
                    Ok(None) => {}
                    Err(ValidationError(msg)) => {
                        return Err(ServerError::BadRequest(format!(
                            "failed validation for the {} spec:\n{}",
                            spec.name, msg
                        )));
                    }
                }
            }
        }
    }
    Ok((modified, metadata))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn container_create() -> ValidationTarget<'static> {
        ValidationTarget::Create {
            structure_family: StructureFamily::Container,
            structure: None,
        }
    }

    fn accept_validator() -> Validator {
        Arc::new(|_spec, _ctx| Ok(None))
    }

    fn reject_validator() -> Validator {
        Arc::new(|_spec, _ctx| Err(ValidationError::new("nope")))
    }

    #[test]
    fn no_specs_is_a_noop() {
        let reg = ValidationRegistry::new();
        let (modified, meta) =
            validate_specs(&reg, false, &[], json!({"a": 1}), container_create()).unwrap();
        assert!(!modified);
        assert_eq!(meta, json!({"a": 1}));
    }

    #[test]
    fn undeclared_spec_passes_when_reject_off() {
        let reg = ValidationRegistry::new();
        let specs = [Spec::new("mystery")];
        let res = validate_specs(&reg, false, &specs, json!({}), container_create());
        assert!(res.is_ok());
    }

    #[test]
    fn undeclared_spec_rejected_when_reject_on() {
        let reg = ValidationRegistry::new();
        let specs = [Spec::new("mystery")];
        let err = validate_specs(&reg, true, &specs, json!({}), container_create()).unwrap_err();
        match err {
            ServerError::BadRequest(msg) => assert_eq!(msg, "Unrecognized spec: mystery"),
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn registered_accept_validator_passes() {
        let mut reg = ValidationRegistry::new();
        reg.register(Spec::new("ok"), accept_validator());
        let specs = [Spec::new("ok")];
        let (modified, _) =
            validate_specs(&reg, true, &specs, json!({}), container_create()).unwrap();
        assert!(!modified);
    }

    #[test]
    fn registered_reject_validator_fails_with_spec_name_in_detail() {
        let mut reg = ValidationRegistry::new();
        reg.register(Spec::new("strict"), reject_validator());
        let specs = [Spec::new("strict")];
        let err = validate_specs(&reg, false, &specs, json!({}), container_create()).unwrap_err();
        match err {
            ServerError::BadRequest(msg) => {
                assert!(
                    msg.contains("failed validation for the strict spec"),
                    "{msg}"
                );
                assert!(msg.contains("nope"), "{msg}");
            }
            other => panic!("expected BadRequest, got {other:?}"),
        }
    }

    #[test]
    fn validator_may_normalize_metadata() {
        let mut reg = ValidationRegistry::new();
        reg.register(
            Spec::new("norm"),
            Arc::new(|_spec, ctx| {
                let mut m = ctx.metadata.clone();
                m["added"] = json!(true);
                Ok(Some(m))
            }),
        );
        let specs = [Spec::new("norm")];
        let (modified, meta) =
            validate_specs(&reg, false, &specs, json!({"a": 1}), container_create()).unwrap();
        assert!(modified);
        assert_eq!(meta, json!({"a": 1, "added": true}));
    }

    // Reverse order: the LEAST-constrained spec (listed last) runs first, so its
    // normalization is visible to the more-constrained spec (listed first).
    #[test]
    fn specs_validated_in_reverse_order() {
        let mut reg = ValidationRegistry::new();
        // "loose" (listed last) adds `flag`; "strict" (listed first) requires it.
        reg.register(
            Spec::new("loose"),
            Arc::new(|_spec, ctx| {
                let mut m = ctx.metadata.clone();
                m["flag"] = json!(true);
                Ok(Some(m))
            }),
        );
        reg.register(
            Spec::new("strict"),
            Arc::new(|_spec, ctx| {
                if ctx.metadata.get("flag") == Some(&json!(true)) {
                    Ok(None)
                } else {
                    Err(ValidationError::new("flag missing"))
                }
            }),
        );
        let specs = [Spec::new("strict"), Spec::new("loose")];
        let res = validate_specs(&reg, false, &specs, json!({}), container_create());
        assert!(
            res.is_ok(),
            "reverse order should let loose normalize before strict runs"
        );
    }

    // A registered spec with a version does not match a bare (version-None)
    // registration — lookup is by the full Spec, matching upstream.
    #[test]
    fn versioned_spec_does_not_match_unversioned_registration() {
        let mut reg = ValidationRegistry::new();
        reg.register(Spec::new("thing"), reject_validator());
        // Node carries thing@1.0 — not the registered thing@None.
        let specs = [Spec::with_version("thing", "1.0")];
        // reject_undeclared off → the versioned spec is simply undeclared, passes.
        assert!(validate_specs(&reg, false, &specs, json!({}), container_create()).is_ok());
        // reject_undeclared on → the versioned spec is unrecognized → 400.
        assert!(validate_specs(&reg, true, &specs, json!({}), container_create()).is_err());
    }
}
