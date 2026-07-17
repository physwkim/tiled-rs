//! Offline compiler for tag-based access-control config.
//!
//! Faithful port of Python tiled's `AccessTagsCompiler`
//! (`tiled/access_control/access_tags.py`). It takes a tag config — `roles`,
//! `tags` (each with `users`, `groups`, and nested `auto_tags`), and
//! `tag_owners` — supplied either inline or as a YAML file, and compiles it
//! into a self-contained SQLite database that a tag-based access policy can
//! query by user name:
//!
//!   * `tags(id, name, is_public)` — every defined tag; `is_public = 1` when
//!     the tag (transitively, via an `auto_tags: [public]`) inherits the
//!     built-in `public` tag.
//!   * `users(id, name)` / `scopes(id, name)` — interned name tables.
//!   * `tags_users_scopes(tag_id, user_id, scope_id)` — the per-`(user, tag)`
//!     scope grant. This is the richness the flat `principals.access_tags`
//!     model cannot express: two users may hold *different* scopes on the same
//!     tag (e.g. one via a `write` role, another via a read-only group).
//!   * `tag_owners(tag_id, user_id)` — who may (un)assign a tag to a node.
//!   * views `public_tags`, `user_tag_scopes`, `user_tag_owners` for reads.
//!
//! Compilation resolves roles to scope sets, walks each tag's `auto_tags`
//! depth-first (bounded by [`MAX_TAG_NESTING`], cycle-safe), expands groups to
//! member users via a caller-supplied `group_parser`, and propagates the
//! public flag up the nesting chain. The DB write is a single wipe-and-rebuild
//! transaction, so a [`AccessTagsCompiler::recompile`] with the same config is
//! idempotent and one with a changed config applies exactly the delta.
//!
//! ## Runtime consumption is deliberately out of scope
//!
//! This compiler is standalone, mirroring upstream's offline
//! `example_configs/access_tags/compile_tags.py`. The existing
//! [`crate::access::TagBasedPolicy`] reads a *different*, weaker schema
//! (`principals.access_tags` + per-tag `tag_scopes`, keyed by principal UUID).
//! Wiring this compiler's per-`(user, tag, scope)` output into that policy
//! requires an identity decision — upstream ACLs are keyed by
//! identity-provider *username*, the policy by principal UUID — that is still
//! open. This module therefore does not touch the policy or the config
//! `build()` path; it provides the compiler as a library primitive.

use std::collections::{BTreeMap, BTreeSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Deserializer};
use sqlx::SqlitePool;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use thiserror::Error;

/// The built-in, always-public tag. It is seeded into every compiled DB with
/// `is_public = 1` and no user ACL, and it cannot be redefined by config.
/// Mirrors upstream `AccessTagsCompiler.public_tag` (casefolded `"public"`).
const PUBLIC_TAG: &str = "public";

/// Maximum depth of nested `auto_tags` references. Mirrors upstream
/// `AccessTagsCompiler._MAX_TAG_NESTING`. A reference chain deeper than this
/// (a tag reached at nesting level `> MAX_TAG_NESTING`) is a compile error.
pub const MAX_TAG_NESTING: usize = 5;

/// Resolve a group name to its member user names.
///
/// `Some(members)` — the group exists (an empty vec is a real, memberless
/// group). `None` — the group does not exist; the reference is warned about
/// and skipped, mirroring upstream's `KeyError` → `warnings.warn` path.
pub type GroupParser = Arc<dyn Fn(&str) -> Option<Vec<String>> + Send + Sync>;

// ---- Config model ----------------------------------------------------------

/// Where the raw tag config is read from.
#[derive(Debug, Clone)]
pub enum TagConfigSource {
    /// A YAML file on disk. Read + parsed lazily by
    /// [`AccessTagsCompiler::load_tag_config`].
    File(std::path::PathBuf),
    /// An already-parsed config (e.g. lifted from the server config document).
    Inline(TagConfig),
}

/// The parsed tag config document. `tags` is required (a config with no
/// `tags` block is an error, matching upstream's `tag_definitions["tags"]`
/// dict access); `roles` and `tag_owners` default to empty.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TagConfig {
    #[serde(default)]
    pub roles: BTreeMap<String, RoleDef>,
    pub tags: BTreeMap<String, TagDef>,
    #[serde(default)]
    pub tag_owners: BTreeMap<String, OwnerDef>,
}

/// A named role: a reusable scope set referenced by `role:` on a user/group.
#[derive(Debug, Clone, Deserialize)]
pub struct RoleDef {
    /// `None` when the `scopes:` key is absent — a compile error, since a role
    /// with no scopes is meaningless. Validated in [`AccessTagsCompiler::compile`].
    #[serde(default)]
    pub scopes: Option<Vec<String>>,
}

/// A tag definition: direct `users`, `groups`, and nested `auto_tags`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct TagDef {
    #[serde(default)]
    pub users: Vec<Member>,
    #[serde(default)]
    pub groups: Vec<Member>,
    #[serde(default)]
    pub auto_tags: Vec<AutoTag>,
}

/// A user or group entry inside a tag's `users` / `groups` list. Exactly one
/// of the `role` / `scopes` *keys* must be present (enforced at compile time).
///
/// `role` and `scopes` are `Option<Option<_>>` so that a *present-but-null* key
/// (`role:` / `scopes:` with no value) is distinguishable from an *absent* one:
/// absent → `None`, `key:` → `Some(None)`, `key: value` → `Some(Some(value))`.
/// Upstream keys the both/neither validation on YAML key *presence*
/// (`all(k in user for k in ("scopes", "role"))` under a presence-only load),
/// so a blanked `scopes:` alongside a real `role:` must still count as "both
/// keys present" and be rejected — see [`AccessTagsCompiler::resolve_member_scopes`].
#[derive(Debug, Clone, Deserialize)]
pub struct Member {
    pub name: String,
    #[serde(default, deserialize_with = "double_option")]
    pub role: Option<Option<String>>,
    #[serde(default, deserialize_with = "double_option")]
    pub scopes: Option<Option<Vec<String>>>,
}

/// Deserialize an optional field so that an absent key and a present-but-null
/// key stay distinguishable. serde only calls `deserialize_with` when the key
/// is present, and `#[serde(default)]` supplies `None` when it is absent; so a
/// present key deserializes its (possibly null) inner value and wraps it in
/// `Some`, yielding: absent → `None`, `key:` (null) → `Some(None)`,
/// `key: value` → `Some(Some(value))`.
fn double_option<'de, T, D>(de: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: Deserializer<'de>,
{
    Deserialize::deserialize(de).map(Some)
}

/// A nested-tag reference inside `auto_tags`.
#[derive(Debug, Clone, Deserialize)]
pub struct AutoTag {
    pub name: String,
}

/// Ownership grant for a tag: the users/groups permitted to (un)assign it.
/// Owners carry no scopes — ownership is a binary capability.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct OwnerDef {
    #[serde(default)]
    pub users: Vec<OwnerMember>,
    #[serde(default)]
    pub groups: Vec<OwnerMember>,
}

/// A user or group name in a `tag_owners` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct OwnerMember {
    pub name: String,
}

// ---- Errors ----------------------------------------------------------------

#[derive(Debug, Error)]
pub enum AccessTagsError {
    #[error("database: {0}")]
    Db(#[from] sqlx::Error),

    #[error("failed to read tag config file {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("the tag config file {0} doesn't exist")]
    ConfigFileMissing(String),

    #[error("failed to parse tag config YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("'public' tag cannot be redefined")]
    PublicRedefined,

    #[error("tag {tag:?} has nested auto_tag {auto_tag:?} which has no definition")]
    UndefinedAutoTag { tag: String, auto_tag: String },

    #[error("scopes must be defined for role {role:?}")]
    RoleScopesMissing { role: String },

    #[error("scopes must not be empty for role {role:?}")]
    RoleScopesEmpty { role: String },

    #[error("scopes for role {role:?} are not in the valid set of scopes; invalid: {invalid:?}")]
    RoleScopesInvalid { role: String, invalid: Vec<String> },

    #[error("cannot define both 'scopes' and 'role' for {kind} {name:?}")]
    BothRoleAndScopes { kind: &'static str, name: String },

    #[error("must define either 'scopes' or 'role' for {kind} {name:?}")]
    NeitherRoleNorScopes { kind: &'static str, name: String },

    #[error("scopes must not be empty for {kind} {name:?}")]
    EmptyScopes { kind: &'static str, name: String },

    #[error("scopes for {kind} {name:?} are not in the valid set of scopes; invalid: {invalid:?}")]
    InvalidScopes {
        kind: &'static str,
        name: String,
        invalid: Vec<String>,
    },

    #[error("exceeded maximum tag nesting of {max} levels")]
    MaxNestingExceeded { max: usize },

    #[error("tag compilation failed at tag {tag:?}: {source}")]
    Compile {
        tag: String,
        #[source]
        source: Box<AccessTagsError>,
    },
}

// ---- Compiler --------------------------------------------------------------

/// Read-only inputs threaded through the `auto_tags` DFS.
struct CompileInputs<'a> {
    adjacency: &'a BTreeMap<String, BTreeSet<String>>,
    tags: &'a BTreeMap<String, TagDef>,
    roles: &'a BTreeMap<String, RoleDef>,
    valid_scopes: &'a BTreeSet<String>,
    group_parser: &'a Option<GroupParser>,
    max_nesting: usize,
}

/// Accumulated DFS outputs: per-tag `(user -> scopes)` ACL and the set of
/// tags that resolved to public.
struct CompileOutputs {
    compiled_tags: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
    compiled_public: BTreeSet<String>,
}

/// The tag-config compiler. Holds the valid scope set, the raw (loaded) config,
/// the group parser, and an owned SQLite connection pool for the compiled DB.
pub struct AccessTagsCompiler {
    scopes: BTreeSet<String>,
    tag_config: TagConfigSource,
    group_parser: Option<GroupParser>,
    roles: BTreeMap<String, RoleDef>,
    tags: BTreeMap<String, TagDef>,
    tag_owners: BTreeMap<String, OwnerDef>,
    pool: SqlitePool,
}

impl AccessTagsCompiler {
    /// Connect to (creating if missing) the compiled-tags SQLite DB at `db_uri`
    /// and create the schema. The raw config is not read until
    /// [`Self::load_tag_config`] is called, matching upstream's split of
    /// `__init__` (opens DB, creates tables) from `load_tag_config`.
    ///
    /// A single-connection pool is used: the DB (a `sqlite::memory:` or file)
    /// is a batch artifact, and a size-1 pool keeps an in-memory DB alive and
    /// consistent for the compiler's lifetime.
    pub async fn connect(
        scopes: BTreeSet<String>,
        tag_config: TagConfigSource,
        db_uri: &str,
        group_parser: Option<GroupParser>,
    ) -> Result<Self, AccessTagsError> {
        let opts = SqliteConnectOptions::from_str(db_uri)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .min_connections(1)
            .idle_timeout(None)
            .max_lifetime(None)
            .connect_with(opts)
            .await?;
        let compiler = Self {
            scopes,
            tag_config,
            group_parser,
            roles: BTreeMap::new(),
            tags: BTreeMap::new(),
            tag_owners: BTreeMap::new(),
            pool,
        };
        compiler.create_tables().await?;
        Ok(compiler)
    }

    /// The compiled-tags DB pool, for a reader/parser or tests.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// The loaded `roles` (roles are not persisted to the DB; upstream keeps
    /// them only in memory). Exposed so a caller can inspect the loaded config.
    pub fn roles(&self) -> &BTreeMap<String, RoleDef> {
        &self.roles
    }

    /// Replace the config source (e.g. to recompile a mutated config).
    pub fn set_tag_config(&mut self, tag_config: TagConfigSource) {
        self.tag_config = tag_config;
    }

    /// Replace the group parser (e.g. group membership changed).
    pub fn set_group_parser(&mut self, group_parser: Option<GroupParser>) {
        self.group_parser = group_parser;
    }

    /// Read the config source and merge it into the loaded `roles`/`tags`/
    /// `tag_owners` (mirrors upstream `dict.update` accumulation). Call
    /// [`Self::clear_raw_tags`] first for a clean reload.
    pub fn load_tag_config(&mut self) -> Result<(), AccessTagsError> {
        let cfg = match &self.tag_config {
            TagConfigSource::Inline(c) => c.clone(),
            TagConfigSource::File(path) => {
                let text = std::fs::read_to_string(path).map_err(|e| {
                    if e.kind() == std::io::ErrorKind::NotFound {
                        AccessTagsError::ConfigFileMissing(path.display().to_string())
                    } else {
                        AccessTagsError::Io {
                            path: path.display().to_string(),
                            source: e,
                        }
                    }
                })?;
                serde_yaml::from_str(&text)?
            }
        };
        self.roles.extend(cfg.roles);
        self.tags.extend(cfg.tags);
        self.tag_owners.extend(cfg.tag_owners);
        Ok(())
    }

    /// Drop the loaded raw config. Pair with [`Self::load_tag_config`] to
    /// replace (rather than merge) before a [`Self::recompile`].
    pub fn clear_raw_tags(&mut self) {
        self.roles.clear();
        self.tags.clear();
        self.tag_owners.clear();
    }

    /// Compile the loaded config and write the result into the DB. Idempotent:
    /// it recomputes the compiled state from the raw config from scratch each
    /// call and rebuilds the DB in a single transaction, so calling it twice
    /// with the same config leaves the DB unchanged.
    pub async fn compile(&mut self) -> Result<(), AccessTagsError> {
        let mut out = CompileOutputs {
            // The built-in public tag: present, public, no user ACL.
            compiled_tags: BTreeMap::from([(PUBLIC_TAG.to_string(), BTreeMap::new())]),
            compiled_public: BTreeSet::from([PUBLIC_TAG.to_string()]),
        };
        let mut compiled_tag_owners: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        // 1. Validate every role: scopes present, non-empty, and a subset of
        //    the valid scope set. Roles are validated even if never referenced.
        for (role_name, role) in &self.roles {
            match &role.scopes {
                None => {
                    return Err(AccessTagsError::RoleScopesMissing {
                        role: role_name.clone(),
                    });
                }
                Some(scopes) if scopes.is_empty() => {
                    return Err(AccessTagsError::RoleScopesEmpty {
                        role: role_name.clone(),
                    });
                }
                Some(scopes) => {
                    let set: BTreeSet<String> = scopes.iter().cloned().collect();
                    let invalid: Vec<String> = set.difference(&self.scopes).cloned().collect();
                    if !invalid.is_empty() {
                        return Err(AccessTagsError::RoleScopesInvalid {
                            role: role_name.clone(),
                            invalid,
                        });
                    }
                }
            }
        }

        // 2. Build the auto_tags adjacency. `public` cannot be redefined as a
        //    tag; every auto_tag must reference a defined tag (or `public`).
        let mut adjacency: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (tag, def) in &self.tags {
            if tag.to_lowercase() == PUBLIC_TAG {
                return Err(AccessTagsError::PublicRedefined);
            }
            let mut adj = BTreeSet::new();
            for auto_tag in &def.auto_tags {
                if !self.tags.contains_key(&auto_tag.name)
                    && auto_tag.name.to_lowercase() != PUBLIC_TAG
                {
                    return Err(AccessTagsError::UndefinedAutoTag {
                        tag: tag.clone(),
                        auto_tag: auto_tag.name.clone(),
                    });
                }
                adj.insert(auto_tag.name.clone());
            }
            adjacency.insert(tag.clone(), adj);
        }

        // 3. DFS each tag to resolve its ACL and public status.
        let inputs = CompileInputs {
            adjacency: &adjacency,
            tags: &self.tags,
            roles: &self.roles,
            valid_scopes: &self.scopes,
            group_parser: &self.group_parser,
            max_nesting: MAX_TAG_NESTING,
        };
        for tag in adjacency.keys() {
            let mut seen = BTreeSet::new();
            Self::dfs(&inputs, &mut out, &mut seen, tag, 0).map_err(|e| {
                AccessTagsError::Compile {
                    tag: tag.clone(),
                    source: Box::new(e),
                }
            })?;
        }

        // 4. Compile tag ownership (users + expanded groups). A `tag_owners`
        //    entry always materializes the tag, even with no owner users.
        for (tag, owner) in &self.tag_owners {
            let entry = compiled_tag_owners.entry(tag.clone()).or_default();
            for user in &owner.users {
                entry.insert(user.name.clone());
            }
            for group in &owner.groups {
                match resolve_group(&self.group_parser, &group.name) {
                    Some(members) => {
                        for username in members {
                            entry.insert(username);
                        }
                    }
                    None => {
                        tracing::warn!(
                            "Group with groupname={:?} does not exist - skipping",
                            group.name
                        );
                    }
                }
            }
        }

        // 5. Rebuild the DB atomically.
        self.write_to_db(
            &out.compiled_tags,
            &compiled_tag_owners,
            &out.compiled_public,
        )
        .await
    }

    /// Alias for [`Self::compile`], kept for parity with upstream's API. Because
    /// `compile` already recomputes from scratch, no separate reset is needed.
    pub async fn recompile(&mut self) -> Result<(), AccessTagsError> {
        self.compile().await
    }

    /// Depth-first resolution of one tag's ACL, following `auto_tags`.
    ///
    /// Returns `(users -> scopes, is_public)`. Memoized via
    /// `out.compiled_tags`; cycle-safe via `seen` (a tag already on the current
    /// path contributes nothing and does not recurse); depth-bounded by
    /// `max_nesting`.
    fn dfs(
        inputs: &CompileInputs<'_>,
        out: &mut CompileOutputs,
        seen: &mut BTreeSet<String>,
        current: &str,
        nested_level: usize,
    ) -> Result<(BTreeMap<String, BTreeSet<String>>, bool), AccessTagsError> {
        if let Some(cached) = out.compiled_tags.get(current) {
            return Ok((cached.clone(), out.compiled_public.contains(current)));
        }
        if seen.contains(current) {
            return Ok((BTreeMap::new(), false));
        }
        if nested_level > inputs.max_nesting {
            return Err(AccessTagsError::MaxNestingExceeded {
                max: inputs.max_nesting,
            });
        }
        seen.insert(current.to_string());

        let mut users: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut is_public = false;

        if let Some(children) = inputs.adjacency.get(current) {
            for child in children {
                if child.to_lowercase() == PUBLIC_TAG {
                    is_public = true;
                    continue;
                }
                let (child_users, child_public) =
                    Self::dfs(inputs, out, seen, child, nested_level + 1).map_err(|e| {
                        AccessTagsError::Compile {
                            tag: current.to_string(),
                            source: Box::new(e),
                        }
                    })?;
                is_public = is_public || child_public;
                // DELIBERATE DEVIATION FROM UPSTREAM: upstream merges an
                // inherited child ACL with `dict.update`, which (a) overwrites a
                // shared user's scope set with the last child's and (b) aliases
                // the child's cached set so a later parent-level mutation
                // corrupts the child. We UNION instead, so a user reachable via
                // two sibling `auto_tags` keeps the scopes from both, and each
                // tag owns its own scope sets. No upstream test exercises a user
                // shared across sibling auto_tags, so this is observably
                // identical on real configs while dropping the latent bug.
                for (username, scopes) in child_users {
                    users.entry(username).or_default().extend(scopes);
                }
            }
        }

        if is_public {
            out.compiled_public.insert(current.to_string());
        }

        // The tag's own direct users and groups are unioned on top of the
        // inherited ACL.
        if let Some(def) = inputs.tags.get(current) {
            for member in &def.users {
                let scopes =
                    Self::resolve_member_scopes(member, inputs.roles, inputs.valid_scopes, "user")?;
                users.entry(member.name.clone()).or_default().extend(scopes);
            }
            for group in &def.groups {
                // Scope validation runs before group expansion, matching
                // upstream: a bad group scope is an error even if the group
                // does not exist.
                let scopes =
                    Self::resolve_member_scopes(group, inputs.roles, inputs.valid_scopes, "group")?;
                match resolve_group(inputs.group_parser, &group.name) {
                    Some(members) => {
                        for username in members {
                            users
                                .entry(username)
                                .or_default()
                                .extend(scopes.iter().cloned());
                        }
                    }
                    None => {
                        tracing::warn!(
                            "Group with groupname={:?} does not exist - skipping",
                            group.name
                        );
                    }
                }
            }
        }

        out.compiled_tags.insert(current.to_string(), users.clone());
        Ok((users, is_public))
    }

    /// Resolve a user/group entry to its scope set. Exactly one of `role` /
    /// `scopes` must be present; the resolved set must be non-empty and a
    /// subset of the valid scopes. An unknown role name resolves to an empty
    /// set (→ "scopes must not be empty"), matching upstream.
    fn resolve_member_scopes(
        member: &Member,
        roles: &BTreeMap<String, RoleDef>,
        valid_scopes: &BTreeSet<String>,
        kind: &'static str,
    ) -> Result<BTreeSet<String>, AccessTagsError> {
        // Key *presence* — not value non-nullness — drives the both/neither
        // gate, matching upstream's `all(k in user for k in ("scopes","role"))`.
        // A present-but-null `role:`/`scopes:` (`Some(None)`) counts as present,
        // so e.g. a real `role:` beside a blanked `scopes:` is rejected as "both".
        let has_role = member.role.is_some();
        let has_scopes = member.scopes.is_some();
        if has_role && has_scopes {
            return Err(AccessTagsError::BothRoleAndScopes {
                kind,
                name: member.name.clone(),
            });
        }
        if !has_role && !has_scopes {
            return Err(AccessTagsError::NeitherRoleNorScopes {
                kind,
                name: member.name.clone(),
            });
        }
        // Exactly one key present. Resolve it to a scope set; a present-but-null
        // value (or a `role:` naming an unknown/undefined role) resolves to the
        // empty set, which trips the "must not be empty" guard below — matching
        // upstream, where such cases fall through to `user.get("scopes", [])`.
        let scopes: BTreeSet<String> = match &member.role {
            // `role:` key present — resolve via the named role.
            Some(role) => role
                .as_deref()
                .and_then(|name| roles.get(name))
                .and_then(|def| def.scopes.clone())
                .unwrap_or_default()
                .into_iter()
                .collect(),
            // `role:` key absent — the gate guarantees `scopes:` is present.
            None => member
                .scopes
                .clone()
                .flatten()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        };
        if scopes.is_empty() {
            return Err(AccessTagsError::EmptyScopes {
                kind,
                name: member.name.clone(),
            });
        }
        let invalid: Vec<String> = scopes.difference(valid_scopes).cloned().collect();
        if !invalid.is_empty() {
            return Err(AccessTagsError::InvalidScopes {
                kind,
                name: member.name.clone(),
                invalid,
            });
        }
        Ok(scopes)
    }

    /// Create the compiled-tags schema (tables, indexes, views). Idempotent.
    async fn create_tables(&self) -> Result<(), AccessTagsError> {
        sqlx::raw_sql(SCHEMA_SQL).execute(&self.pool).await?;
        Ok(())
    }

    /// Rebuild the compiled-tags DB from the compiled sets in one transaction.
    ///
    /// This wipe-and-rebuild achieves the same observable outcome as upstream's
    /// staged temp-table upsert (the prod tables end equal to the compiled
    /// sets, and a re-run with the same input is a no-op) while being far
    /// simpler. The internal integer ids are ephemeral — nothing outside this
    /// DB references them — so recreating them each compile is safe.
    async fn write_to_db(
        &self,
        compiled_tags: &BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
        compiled_tag_owners: &BTreeMap<String, BTreeSet<String>>,
        compiled_public: &BTreeSet<String>,
    ) -> Result<(), AccessTagsError> {
        let mut tx = self.pool.begin().await?;

        // Relationship tables first so no FK ever dangles during the wipe.
        for stmt in [
            "DELETE FROM tags_users_scopes",
            "DELETE FROM tag_owners",
            "DELETE FROM tags",
            "DELETE FROM users",
            "DELETE FROM scopes",
        ] {
            sqlx::query(stmt).execute(&mut *tx).await?;
        }

        // scopes: the full valid set (upstream inserts all valid scopes, not
        // only the referenced ones).
        for scope in &self.scopes {
            sqlx::query("INSERT INTO scopes(name) VALUES (?)")
                .bind(scope)
                .execute(&mut *tx)
                .await?;
        }

        // tags: union of ACL tags and owner-only tags. is_public from the set.
        let mut tag_names: BTreeSet<&String> = compiled_tags.keys().collect();
        tag_names.extend(compiled_tag_owners.keys());
        for tag in tag_names {
            let is_public: i64 = if compiled_public.contains(tag) { 1 } else { 0 };
            sqlx::query("INSERT INTO tags(name, is_public) VALUES (?, ?)")
                .bind(tag)
                .bind(is_public)
                .execute(&mut *tx)
                .await?;
        }

        // users: union of ACL users and owner users.
        let mut user_names: BTreeSet<&String> = BTreeSet::new();
        for acl in compiled_tags.values() {
            user_names.extend(acl.keys());
        }
        for owners in compiled_tag_owners.values() {
            user_names.extend(owners.iter());
        }
        for user in user_names {
            sqlx::query("INSERT INTO users(name) VALUES (?)")
                .bind(user)
                .execute(&mut *tx)
                .await?;
        }

        // Relationships, resolving ids by name via INSERT ... SELECT so no
        // id-map round-trip (and no panic path) is needed. Every referenced
        // name was inserted above, so each SELECT yields exactly one row.
        for (tag, acl) in compiled_tags {
            for (user, scopes) in acl {
                for scope in scopes {
                    sqlx::query(
                        "INSERT INTO tags_users_scopes(tag_id, user_id, scope_id) \
                         SELECT t.id, u.id, s.id FROM tags t, users u, scopes s \
                         WHERE t.name = ? AND u.name = ? AND s.name = ?",
                    )
                    .bind(tag)
                    .bind(user)
                    .bind(scope)
                    .execute(&mut *tx)
                    .await?;
                }
            }
        }
        for (tag, owners) in compiled_tag_owners {
            for user in owners {
                sqlx::query(
                    "INSERT INTO tag_owners(tag_id, user_id) \
                     SELECT t.id, u.id FROM tags t, users u \
                     WHERE t.name = ? AND u.name = ?",
                )
                .bind(tag)
                .bind(user)
                .execute(&mut *tx)
                .await?;
            }
        }

        tx.commit().await?;
        Ok(())
    }
}

/// Resolve a group via the optional parser. `None` (no parser, or the parser
/// reports the group missing) means "skip this group with a warning".
fn resolve_group(parser: &Option<GroupParser>, name: &str) -> Option<Vec<String>> {
    parser.as_ref().and_then(|p| p(name))
}

/// The valid scope names a tag config may reference — every real permission
/// scope (mirrors upstream `ALL_SCOPES`). The tiled-rs-internal `inherit` and
/// `admin` metascopes are excluded: they are not assignable permissions.
pub fn all_scope_names() -> BTreeSet<String> {
    crate::auth::Scope::ALL
        .iter()
        .filter(|s| !matches!(s, crate::auth::Scope::Inherit | crate::auth::Scope::Admin))
        .map(|s| s.as_str().to_string())
        .collect()
}

/// Compiled-tags schema. Mirrors upstream `create_access_tags_tables`
/// (`access_tags.py`), minus the connection PRAGMAs sqlx applies at connect.
const SCHEMA_SQL: &str = "\
CREATE TABLE IF NOT EXISTS tags (
  id        INTEGER PRIMARY KEY,
  name      TEXT    UNIQUE NOT NULL,
  is_public INTEGER NOT NULL DEFAULT 0
    CHECK (is_public IN (0,1))
);
CREATE TABLE IF NOT EXISTS users (
  id   INTEGER PRIMARY KEY,
  name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS scopes (
  id   INTEGER PRIMARY KEY,
  name TEXT    UNIQUE NOT NULL
);
CREATE TABLE IF NOT EXISTS tags_users_scopes (
  tag_id    INTEGER NOT NULL
    REFERENCES tags(id) ON UPDATE CASCADE ON DELETE CASCADE,
  user_id   INTEGER NOT NULL
    REFERENCES users(id) ON UPDATE CASCADE ON DELETE CASCADE,
  scope_id  INTEGER NOT NULL
    REFERENCES scopes(id) ON UPDATE CASCADE ON DELETE CASCADE,
  PRIMARY KEY (tag_id, user_id, scope_id)
);
CREATE TABLE IF NOT EXISTS tag_owners (
  tag_id   INTEGER NOT NULL
    REFERENCES tags(id) ON UPDATE CASCADE ON DELETE CASCADE,
  user_id  INTEGER NOT NULL
    REFERENCES users(id) ON UPDATE CASCADE ON DELETE CASCADE,
  PRIMARY KEY (tag_id, user_id)
);
CREATE INDEX IF NOT EXISTS idx_tags_is_public ON tags (is_public);
CREATE INDEX IF NOT EXISTS idx_tus_users_scopes ON tags_users_scopes (user_id, scope_id);
CREATE INDEX IF NOT EXISTS idx_tus_users_scopes_scopeid ON tags_users_scopes (scope_id);
CREATE INDEX IF NOT EXISTS idx_tag_owners ON tag_owners (user_id);
CREATE VIEW IF NOT EXISTS public_tags AS
  SELECT name FROM tags WHERE is_public = 1;
CREATE VIEW IF NOT EXISTS user_tag_scopes AS
  SELECT
    u.name AS user_name,
    t.name AS tag_name,
    s.name AS scope_name
  FROM tags_users_scopes tus
    JOIN users  u ON u.id = tus.user_id
    JOIN tags   t ON t.id = tus.tag_id
    JOIN scopes s ON s.id = tus.scope_id;
CREATE VIEW IF NOT EXISTS user_tag_owners AS
  SELECT
    u.name AS user_name,
    t.name AS tag_name
  FROM tag_owners towner
    JOIN users u ON u.id = towner.user_id
    JOIN tags  t ON t.id = towner.tag_id;
";

#[cfg(test)]
mod tests;
