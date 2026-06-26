//! LDAP authenticator — validates `(username, password)` by binding to an LDAP
//! directory.
//!
//! Faithful port of Python tiled's `LDAPAuthenticator` (`authenticators.py`),
//! which is itself based on jupyterhub's `ldapauthenticator`. The decision flow
//! mirrors the Python original:
//!
//! 1. Reject usernames that do not match `valid_username_regex` (LDAP-injection
//!    guard) and blank passwords.
//! 2. Optionally resolve the user's DN via a technical search account
//!    (`lookup_dn`).
//! 3. Bind as the user, trying each `bind_dn_template` against each configured
//!    server until one succeeds.
//! 4. Optionally require the bound user to match `search_filter` (exactly one
//!    entry) and/or be a member of one of `allowed_groups`.
//! 5. Yield `Subject { provider, sub: username }`.
//!
//! Gated on the `ldap` feature (pure-Rust `ldap3` with rustls TLS).
//!
//! Parity note: Python's `auth_state_attributes` are searched and returned in
//! `UserSessionState.state`. The Rust [`Subject`] carries only `(provider,
//! sub)` and has no per-session attribute bag, so `auth_state_attributes` is
//! accepted in config for forward-compatibility but not surfaced; it never
//! affected the authentication decision in Python either.

use std::time::Duration;

use async_trait::async_trait;
use ldap3::{Ldap, LdapConnAsync, LdapConnSettings, Scope, SearchEntry, ldap_escape};
use regex::Regex;
use serde::Deserialize;

use crate::authenticator::{Authenticator, Subject};
use crate::error::{AuthError, Result};

/// A YAML scalar that may be a single string or a list of strings (Python
/// accepts both for `server_address`, `bind_dn_template`, `allowed_groups`,
/// `attributes`, `auth_state_attributes`).
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum StringOrVec {
    /// A single value.
    One(String),
    /// A list of values.
    Many(Vec<String>),
}

impl StringOrVec {
    fn into_vec(self) -> Vec<String> {
        match self {
            StringOrVec::One(s) => vec![s],
            StringOrVec::Many(v) => v,
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_connect_timeout() -> f64 {
    5.0
}
fn default_receive_timeout() -> f64 {
    60.0
}
fn default_username_regex() -> String {
    r"^[a-z][.a-z0-9_-]*$".to_string()
}
fn default_lookup_filter() -> String {
    "({login_attr}={login})".to_string()
}

/// `args` for an LDAP provider. Field names and defaults mirror the constructor
/// kwargs of Python's `LDAPAuthenticator`.
#[derive(Debug, Clone, Deserialize)]
pub struct LdapConfig {
    /// One server, or a list of servers. An address may embed a port
    /// (`host:port`); otherwise `server_port` (or the scheme default) is used.
    pub server_address: StringOrVec,
    /// Port used for addresses that do not embed one. Defaults to 636 when
    /// `use_ssl`, else 389.
    #[serde(default)]
    pub server_port: Option<u16>,
    /// Connect over LDAPS (implicit TLS). Deprecated in LDAPv3 in favor of TLS.
    #[serde(default)]
    pub use_ssl: bool,
    /// Issue STARTTLS before binding (when `use_ssl` is false). Default true.
    #[serde(default = "default_true")]
    pub use_tls: bool,
    /// Connection timeout, seconds. Default 5.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout: f64,
    /// Per-operation timeout, seconds. Default 60.
    #[serde(default = "default_receive_timeout")]
    pub receive_timeout: f64,
    /// DN template(s) with a `{username}` placeholder. One or a list.
    #[serde(default)]
    pub bind_dn_template: Option<StringOrVec>,
    /// If set, the bound user must be a member of one of these group DNs.
    #[serde(default)]
    pub allowed_groups: Option<StringOrVec>,
    /// Regex usernames must match (LDAP-injection guard).
    #[serde(default = "default_username_regex")]
    pub valid_username_regex: String,
    /// Resolve the user's DN via a directory search rather than a template.
    #[serde(default)]
    pub lookup_dn: bool,
    /// Search base for `lookup_dn`.
    #[serde(default)]
    pub user_search_base: Option<String>,
    /// Attribute holding the username (e.g. `uid`, `sAMAccountName`).
    #[serde(default)]
    pub user_attribute: Option<String>,
    /// Filter for the `lookup_dn` search. `{login_attr}`/`{login}` are
    /// substituted. Default `({login_attr}={login})`.
    #[serde(default = "default_lookup_filter")]
    pub lookup_dn_search_filter: String,
    /// Technical account DN for the `lookup_dn` search (None → anonymous).
    #[serde(default)]
    pub lookup_dn_search_user: Option<String>,
    /// Technical account password for the `lookup_dn` search.
    #[serde(default)]
    pub lookup_dn_search_password: Option<String>,
    /// Attribute carrying the value used to build the user DN, for `lookup_dn`.
    #[serde(default)]
    pub lookup_dn_user_dn_attribute: Option<String>,
    /// Escape special characters in the user DN before binding.
    #[serde(default)]
    pub escape_userdn: bool,
    /// Post-bind filter the user must match (`{userattr}`/`{username}`). Empty
    /// disables the check.
    #[serde(default)]
    pub search_filter: String,
    /// Attributes requested by the `search_filter` search.
    #[serde(default)]
    pub attributes: Option<StringOrVec>,
    /// Attributes returned in Python's auth_state (not surfaced here; see the
    /// module docs).
    #[serde(default)]
    pub auth_state_attributes: Option<StringOrVec>,
    /// Use the looked-up DN attribute value as the username instead of the
    /// supplied one. Default true.
    #[serde(default = "default_true")]
    pub use_lookup_dn_username: bool,
    /// May be displayed by the client after a successful login. Parity-only.
    #[serde(default)]
    pub confirmation_message: String,
}

/// Validates credentials by binding to an LDAP directory.
#[derive(Debug)]
pub struct LdapAuthenticator {
    name: String,
    server_address_list: Vec<String>,
    server_port: u16,
    use_ssl: bool,
    use_tls: bool,
    connect_timeout: f64,
    receive_timeout: f64,
    bind_dn_template: Vec<String>,
    allowed_groups: Vec<String>,
    valid_username_regex: Regex,
    lookup_dn: bool,
    user_search_base: Option<String>,
    user_attribute: Option<String>,
    lookup_dn_search_filter: String,
    lookup_dn_search_user: Option<String>,
    lookup_dn_search_password: Option<String>,
    lookup_dn_user_dn_attribute: Option<String>,
    escape_userdn: bool,
    search_filter: String,
    attributes: Vec<String>,
    use_lookup_dn_username: bool,
    confirmation_message: String,
}

impl LdapAuthenticator {
    /// Construct from a provider name and parsed [`LdapConfig`]. Validates that
    /// at least one server is configured and that `valid_username_regex`
    /// compiles, mirroring the eager checks in the Python constructor.
    pub fn from_config(provider_name: impl Into<String>, config: LdapConfig) -> Result<Self> {
        let server_address_list = config.server_address.into_vec();
        if server_address_list.is_empty() {
            return Err(AuthError::Validation(
                "ldap: 'server_address' is an empty list".into(),
            ));
        }
        let valid_username_regex = Regex::new(&config.valid_username_regex).map_err(|e| {
            AuthError::Validation(format!("ldap: invalid 'valid_username_regex': {e}"))
        })?;
        let server_port = config
            .server_port
            .unwrap_or(if config.use_ssl { 636 } else { 389 });

        Ok(Self {
            name: provider_name.into(),
            server_address_list,
            server_port,
            use_ssl: config.use_ssl,
            use_tls: config.use_tls,
            connect_timeout: config.connect_timeout,
            receive_timeout: config.receive_timeout,
            bind_dn_template: config
                .bind_dn_template
                .map(StringOrVec::into_vec)
                .unwrap_or_default(),
            allowed_groups: config
                .allowed_groups
                .map(StringOrVec::into_vec)
                .unwrap_or_default(),
            valid_username_regex,
            lookup_dn: config.lookup_dn,
            user_search_base: config.user_search_base,
            user_attribute: config.user_attribute,
            lookup_dn_search_filter: config.lookup_dn_search_filter,
            lookup_dn_search_user: config.lookup_dn_search_user,
            lookup_dn_search_password: config.lookup_dn_search_password,
            lookup_dn_user_dn_attribute: config.lookup_dn_user_dn_attribute,
            escape_userdn: config.escape_userdn,
            search_filter: config.search_filter,
            attributes: config
                .attributes
                .map(StringOrVec::into_vec)
                .unwrap_or_default(),
            use_lookup_dn_username: config.use_lookup_dn_username,
            confirmation_message: config.confirmation_message,
        })
    }

    /// Parity-only confirmation message.
    pub fn confirmation_message(&self) -> &str {
        &self.confirmation_message
    }

    /// LDAP URLs to try, one per configured server address.
    fn server_urls(&self) -> Vec<String> {
        self.server_address_list
            .iter()
            .map(|addr| server_url(addr, self.server_port, self.use_ssl))
            .collect()
    }

    /// Open a connection to one server. Returns `None` on connect failure so
    /// the caller can try the next server. The returned handle is connected but
    /// not yet bound.
    async fn connect(&self, url: &str) -> Option<Ldap> {
        let settings = LdapConnSettings::new()
            .set_conn_timeout(Duration::from_secs_f64(self.connect_timeout))
            // STARTTLS only when not already on implicit TLS (LDAPS).
            .set_starttls(self.use_tls && !self.use_ssl);
        let (conn, ldap) = LdapConnAsync::with_settings(settings, url).await.ok()?;
        ldap3::drive!(conn);
        Some(ldap)
    }

    fn op_timeout(&self) -> Duration {
        Duration::from_secs_f64(self.receive_timeout)
    }

    /// Resolve the user's DN via the technical search account. Returns the
    /// `(dn-attribute-value, entry-dn)` pair, or `None` when the search account
    /// cannot bind or no/empty entry is found (Python returns `(None, None)`
    /// in these cases, which the caller treats as a failed login).
    async fn resolve_username(&self, supplied_username: &str) -> Option<(String, String)> {
        let search_user = self.lookup_dn_search_user.clone().unwrap_or_default();
        let search_dn = if self.escape_userdn {
            ldap_escape(search_user.as_str()).into_owned()
        } else {
            search_user
        };
        let search_password = self.lookup_dn_search_password.clone().unwrap_or_default();

        // Bind the search account against the first reachable server.
        let mut ldap = None;
        for url in self.server_urls() {
            if let Some(mut conn) = self.connect(&url).await {
                let bound = conn
                    .with_timeout(self.op_timeout())
                    .simple_bind(&search_dn, &search_password)
                    .await
                    .map(|r| r.rc == 0)
                    .unwrap_or(false);
                if bound {
                    ldap = Some(conn);
                    break;
                }
                let _ = conn.unbind().await;
            }
        }
        let mut ldap = ldap?;

        let user_attribute = self.user_attribute.clone().unwrap_or_default();
        let dn_attribute = self.lookup_dn_user_dn_attribute.clone().unwrap_or_default();
        let base = self.user_search_base.clone().unwrap_or_default();
        let filter = self
            .lookup_dn_search_filter
            .replace("{login_attr}", &user_attribute)
            .replace("{login}", supplied_username);

        let result = ldap
            .with_timeout(self.op_timeout())
            .search(&base, Scope::Subtree, &filter, vec![dn_attribute.clone()])
            .await
            .and_then(|r| r.success());
        let _ = ldap.unbind().await;

        let (entries, _res) = result.ok()?;
        let entry = SearchEntry::construct(entries.into_iter().next()?);
        // Python takes the first value when the attribute is multi-valued.
        let user_dn = entry.attrs.get(&dn_attribute)?.first()?.clone();
        Some((user_dn, entry.dn))
    }

    /// Search-filter check: the bound user must match `search_filter`, with
    /// exactly one matching entry. Returns the rejection reason as `Err`.
    async fn passes_search_filter(&self, ldap: &mut Ldap, username: &str) -> Result<()> {
        if self.search_filter.is_empty() {
            return Ok(());
        }
        let user_attribute = self.user_attribute.clone().unwrap_or_default();
        let base = self.user_search_base.clone().unwrap_or_default();
        let filter = self
            .search_filter
            .replace("{userattr}", &user_attribute)
            .replace("{username}", username);
        let (entries, _res) = ldap
            .with_timeout(self.op_timeout())
            .search(&base, Scope::Subtree, &filter, self.attributes.clone())
            .await
            .and_then(|r| r.success())
            .map_err(|e| AuthError::Unauthorized(format!("ldap search failed: {e}")))?;
        // Python rejects when zero OR more than one entry matches.
        if entries.len() == 1 {
            Ok(())
        } else {
            Err(AuthError::Unauthorized(
                "invalid username or password".into(),
            ))
        }
    }

    /// Group-membership check: the bound user must be a member of one of
    /// `allowed_groups`.
    async fn passes_group_check(
        &self,
        ldap: &mut Ldap,
        userdn: &str,
        username: &str,
    ) -> Result<()> {
        if self.allowed_groups.is_empty() {
            return Ok(());
        }
        let filter = build_group_filter(userdn, username);
        let attrs = vec!["member", "uniqueMember", "memberUid"];
        for group in &self.allowed_groups {
            let found = ldap
                .with_timeout(self.op_timeout())
                .search(group, Scope::Base, &filter, attrs.clone())
                .await
                .and_then(|r| r.success())
                .map(|(entries, _)| !entries.is_empty())
                .unwrap_or(false);
            if found {
                return Ok(());
            }
        }
        Err(AuthError::Unauthorized(
            "invalid username or password".into(),
        ))
    }
}

#[async_trait]
impl Authenticator for LdapAuthenticator {
    fn name(&self) -> &str {
        &self.name
    }

    async fn authenticate(&self, username: &str, secret: &str) -> Result<Subject> {
        let username_saved = username.to_string();
        let mut username = username.to_string();
        let password = secret;

        // Injection guard + blank-password rejection (Python order).
        if !username_is_valid(&self.valid_username_regex, &username) {
            return Err(AuthError::Unauthorized(
                "invalid username or password".into(),
            ));
        }
        if password.trim().is_empty() {
            return Err(AuthError::Unauthorized(
                "invalid username or password".into(),
            ));
        }

        let mut bind_dn_template = self.bind_dn_template.clone();
        if !self.lookup_dn && bind_dn_template.is_empty() {
            return Err(AuthError::Unauthorized(
                "ldap: configure 'lookup_dn' or 'bind_dn_template'".into(),
            ));
        }

        if self.lookup_dn {
            let (resolved_username, resolved_dn) = self
                .resolve_username(&username)
                .await
                .ok_or_else(|| AuthError::Unauthorized("invalid username or password".into()))?;
            username = resolved_username;
            if self
                .lookup_dn_user_dn_attribute
                .as_deref()
                .map(|a| a.eq_ignore_ascii_case("CN"))
                .unwrap_or(false)
            {
                // Only escape commas when the lookup attribute is CN.
                username = escape_cn_commas(&username);
            }
            if bind_dn_template.is_empty() {
                bind_dn_template = vec![resolved_dn];
            }
        }

        // Bind loop: first (template, server) that authenticates wins.
        let urls = self.server_urls();
        let mut bound: Option<(Ldap, String)> = None;
        'outer: for dn_template in &bind_dn_template {
            if dn_template.is_empty() {
                continue; // Python: "Ignoring blank 'bind_dn_template' entry!"
            }
            let userdn = format_userdn(dn_template, &username, self.escape_userdn);
            for url in &urls {
                let Some(mut ldap) = self.connect(url).await else {
                    continue; // server unreachable — try the next server
                };
                match ldap
                    .with_timeout(self.op_timeout())
                    .simple_bind(&userdn, password)
                    .await
                {
                    Ok(res) if res.rc == 0 => {
                        bound = Some((ldap, userdn.clone()));
                        break 'outer;
                    }
                    // Bind rejected (e.g. rc=49): the same credentials will not
                    // succeed on another server, so move on to the next DN.
                    _ => {
                        let _ = ldap.unbind().await;
                        break;
                    }
                }
            }
        }

        let (mut ldap, userdn) =
            bound.ok_or_else(|| AuthError::Unauthorized("invalid username or password".into()))?;

        // Post-bind authorization checks. The unbind below must run regardless
        // of the outcome, so capture the result and tear down before returning.
        let result: Result<()> = async {
            self.passes_search_filter(&mut ldap, &username).await?;
            self.passes_group_check(&mut ldap, &userdn, &username)
                .await?;
            Ok(())
        }
        .await;
        let _ = ldap.unbind().await;
        result?;

        if !self.use_lookup_dn_username {
            username = username_saved;
        }

        Ok(Subject {
            provider: self.name.clone(),
            sub: username,
        })
    }
}

// --- Pure helpers (no network) ---------------------------------------------

/// Emulates Python `re.match(regex, username)`: the pattern must match starting
/// at the beginning of the string (its extent is governed by the pattern).
fn username_is_valid(re: &Regex, username: &str) -> bool {
    re.find(username).is_some_and(|m| m.start() == 0)
}

/// Build the user DN from a template, substituting `{username}` and optionally
/// escaping LDAP filter special characters (Python `escape_filter_chars`).
fn format_userdn(template: &str, username: &str, escape: bool) -> String {
    let userdn = template.replace("{username}", username);
    if escape {
        ldap_escape(userdn.as_str()).into_owned()
    } else {
        userdn
    }
}

/// Group-membership filter, matching Python's construction.
fn build_group_filter(userdn: &str, uid: &str) -> String {
    format!("(|(member={userdn})(uniqueMember={userdn})(memberUid={uid}))")
}

/// Escape unescaped commas in a CN value (Python `re.subn(r"([^\\]),",
/// r"\1\,", value)`), used only when the lookup attribute is CN.
fn escape_cn_commas(value: &str) -> String {
    // Single non-overlapping left-to-right pass, like `re.sub`.
    let re = Regex::new(r"([^\\]),").expect("static CN-escape regex");
    re.replace_all(value, "${1}\\,").into_owned()
}

/// Split an address into `(host, port)`, honoring an embedded `host:port`,
/// otherwise using `default_port` (mirrors Python's `address.split(":")`).
fn split_host_port(address: &str, default_port: u16) -> (String, u16) {
    if let Some((host, port)) = address.rsplit_once(':')
        && !host.is_empty()
        && !port.is_empty()
        && port.chars().all(|c| c.is_ascii_digit())
        && let Ok(p) = port.parse::<u16>()
    {
        return (host.to_string(), p);
    }
    (address.to_string(), default_port)
}

/// Build an LDAP URL for one server address.
fn server_url(address: &str, default_port: u16, use_ssl: bool) -> String {
    let (host, port) = split_host_port(address, default_port);
    let scheme = if use_ssl { "ldaps" } else { "ldap" };
    format!("{scheme}://{host}:{port}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(server_address: StringOrVec) -> LdapConfig {
        // Deserialize with all defaults (server_address is required, so seed a
        // placeholder), then override the address under test.
        let mut c: LdapConfig = serde_yaml::from_str("server_address: placeholder").unwrap();
        c.server_address = server_address;
        c
    }

    #[test]
    fn config_defaults_match_python() {
        let c: LdapConfig = serde_yaml::from_str("server_address: ldap.example.org").unwrap();
        assert!(c.use_tls); // default true
        assert!(!c.use_ssl);
        assert_eq!(c.connect_timeout, 5.0);
        assert_eq!(c.receive_timeout, 60.0);
        assert_eq!(c.valid_username_regex, r"^[a-z][.a-z0-9_-]*$");
        assert_eq!(c.lookup_dn_search_filter, "({login_attr}={login})");
        assert!(c.use_lookup_dn_username); // default true
        assert!(!c.lookup_dn);
        assert_eq!(c.server_port, None);
        assert_eq!(c.search_filter, "");
    }

    #[test]
    fn server_address_accepts_string_or_list() {
        let one: LdapConfig = serde_yaml::from_str("server_address: a.example.org").unwrap();
        assert_eq!(one.server_address.into_vec(), vec!["a.example.org"]);
        let many: LdapConfig =
            serde_yaml::from_str("server_address: [a.example.org, b.example.org]").unwrap();
        assert_eq!(
            many.server_address.into_vec(),
            vec!["a.example.org", "b.example.org"]
        );
    }

    #[test]
    fn from_config_rejects_empty_server_list() {
        let c = cfg(StringOrVec::Many(vec![]));
        let err = LdapAuthenticator::from_config("ldap", c).unwrap_err();
        assert!(matches!(err, AuthError::Validation(_)));
    }

    #[test]
    fn from_config_rejects_bad_username_regex() {
        let mut c = cfg(StringOrVec::One("ldap.example.org".into()));
        c.valid_username_regex = "(".into(); // unbalanced
        let err = LdapAuthenticator::from_config("ldap", c).unwrap_err();
        assert!(matches!(err, AuthError::Validation(_)));
    }

    #[test]
    fn server_port_default_follows_use_ssl() {
        let mut plain = cfg(StringOrVec::One("ldap.example.org".into()));
        plain.use_ssl = false;
        let a = LdapAuthenticator::from_config("ldap", plain).unwrap();
        assert_eq!(a.server_port, 389);

        let mut ssl = cfg(StringOrVec::One("ldap.example.org".into()));
        ssl.use_ssl = true;
        let a = LdapAuthenticator::from_config("ldap", ssl).unwrap();
        assert_eq!(a.server_port, 636);
    }

    #[test]
    fn name_is_carried() {
        let a = LdapAuthenticator::from_config(
            "corp-ldap",
            cfg(StringOrVec::One("ldap.example.org".into())),
        )
        .unwrap();
        assert_eq!(a.name(), "corp-ldap");
    }

    #[test]
    fn username_regex_matches_python_re_match() {
        let re = Regex::new(&default_username_regex()).unwrap();
        assert!(username_is_valid(&re, "alice"));
        assert!(username_is_valid(&re, "a.b_c-1"));
        assert!(!username_is_valid(&re, "Alice")); // uppercase first
        assert!(!username_is_valid(&re, "1alice")); // digit first
        assert!(!username_is_valid(&re, "alice bob")); // space
        assert!(!username_is_valid(&re, "alice*")); // illegal char
        assert!(!username_is_valid(&re, "")); // empty
        assert!(!username_is_valid(&re, "a)(uid=*)")); // injection attempt
    }

    #[test]
    fn format_userdn_substitutes_and_optionally_escapes() {
        let t = "uid={username},ou=people,dc=example,dc=org";
        assert_eq!(
            format_userdn(t, "alice", false),
            "uid=alice,ou=people,dc=example,dc=org"
        );
        // With escaping, filter-special chars in the value are escaped.
        let escaped = format_userdn("cn={username}", "a*b", true);
        assert!(escaped.contains("\\2a"), "got {escaped}");
    }

    #[test]
    fn group_filter_is_well_formed() {
        assert_eq!(
            build_group_filter("uid=alice,ou=people,dc=x", "alice"),
            "(|(member=uid=alice,ou=people,dc=x)(uniqueMember=uid=alice,ou=people,dc=x)(memberUid=alice))"
        );
    }

    #[test]
    fn cn_comma_escaping_matches_python() {
        assert_eq!(escape_cn_commas("Doe, John"), "Doe\\, John");
        // An already-escaped comma is left alone.
        assert_eq!(escape_cn_commas("Doe\\, John"), "Doe\\, John");
        assert_eq!(escape_cn_commas("nocomma"), "nocomma");
    }

    #[test]
    fn split_host_port_honors_embedded_port() {
        assert_eq!(split_host_port("host:1389", 389), ("host".into(), 1389));
        assert_eq!(split_host_port("host", 389), ("host".into(), 389));
        // No port digits → default; multiple colons → last segment is the port.
        assert_eq!(split_host_port("a:b:636", 389), ("a:b".into(), 636));
        assert_eq!(
            split_host_port("host:notaport", 389),
            ("host:notaport".into(), 389)
        );
    }

    #[test]
    fn server_url_scheme_follows_use_ssl() {
        assert_eq!(server_url("host", 389, false), "ldap://host:389");
        assert_eq!(server_url("host", 636, true), "ldaps://host:636");
        assert_eq!(server_url("host:1636", 636, true), "ldaps://host:1636");
    }
}
