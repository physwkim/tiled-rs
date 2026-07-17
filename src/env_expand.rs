//! Environment-variable expansion for loaded config / profile YAML.
//!
//! Port of upstream tiled's `expand_environment_variables` (`tiled/utils.py`,
//! itself vendored from `dask.config`), which `parse()` runs over every loaded
//! server-config and client-profile YAML. It walks the parsed value tree and
//! replaces `$VAR` / `${VAR}` inside each string scalar with the matching
//! environment variable — leaving non-string scalars, mapping keys, and any
//! reference to an *unset* variable exactly as written.
//!
//! Single owner: both the server-config loader ([`crate::cli::config`]) and the
//! client-profile loader ([`crate::client::profiles`]) call [`expand_env_vars`]
//! so a `${SECRET_KEY}` in either file is resolved before it reaches the server,
//! rather than being sent literally.

use serde_yaml::Value;

/// Recursively expand `$VAR` / `${VAR}` inside every string in a parsed YAML
/// `value`, in place, using the process environment. Mapping keys are left
/// untouched (upstream expands values only); non-string scalars (numbers,
/// bools, null) are unchanged.
pub fn expand_env_vars(value: &mut Value) {
    expand_with(value, &|name| std::env::var(name).ok());
}

/// [`expand_env_vars`] parameterized by the variable lookup, so the expansion
/// rules can be exercised against a fixed table instead of the racy,
/// process-global real environment.
fn expand_with<F: Fn(&str) -> Option<String>>(value: &mut Value, lookup: &F) {
    match value {
        Value::String(s) => {
            if let Some(expanded) = expand_str(s, lookup) {
                *s = expanded;
            }
        }
        Value::Sequence(seq) => {
            for v in seq.iter_mut() {
                expand_with(v, lookup);
            }
        }
        Value::Mapping(map) => {
            // Values only — keys are preserved verbatim (upstream
            // `{k: expand(v) for k, v in ...}`).
            for (_k, v) in map.iter_mut() {
                expand_with(v, lookup);
            }
        }
        Value::Tagged(tagged) => expand_with(&mut tagged.value, lookup),
        // Null / Bool / Number: non-string scalars are untouched.
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

/// Expand `$VAR` / `${VAR}` in a single string, mirroring Python
/// `os.path.expandvars` (POSIX, `re.ASCII`): a bare `$` is followed by a run of
/// ASCII word characters (`[A-Za-z0-9_]`), or `${...}` names everything up to
/// the first `}`. A reference to an **unset** variable — and any `$` that does
/// not start a valid reference — is left exactly as written. Returns `None`
/// when nothing changed, so the caller keeps the original string allocation.
fn expand_str<F: Fn(&str) -> Option<String>>(s: &str, lookup: &F) -> Option<String> {
    if !s.contains('$') {
        return None;
    }
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    let mut changed = false;
    while i < bytes.len() {
        // Copy the run of ordinary bytes up to the next '$' in one slice. '$'
        // is ASCII, so the slice always ends on a UTF-8 boundary.
        if bytes[i] != b'$' {
            let next = bytes[i..]
                .iter()
                .position(|&b| b == b'$')
                .map_or(bytes.len(), |p| i + p);
            out.push_str(&s[i..next]);
            i = next;
            continue;
        }
        // bytes[i] == '$'
        if bytes.get(i + 1) == Some(&b'{') {
            // ${...}: expand only with a closing brace, matching the
            // `\{[^}]*\}` alternative of upstream's regex.
            if let Some(rel) = bytes[i + 2..].iter().position(|&b| b == b'}') {
                let close = i + 2 + rel;
                let name = &s[i + 2..close];
                match lookup(name) {
                    Some(val) => {
                        out.push_str(&val);
                        changed = true;
                    }
                    // Unset (or empty `${}`) → the reference stays literal.
                    None => out.push_str(&s[i..=close]),
                }
                i = close + 1;
            } else {
                // No closing brace → the '$' is literal.
                out.push('$');
                i += 1;
            }
            continue;
        }
        // $NAME: the maximal run of ASCII word characters after '$'.
        let start = i + 1;
        let mut end = start;
        while end < bytes.len() && (bytes[end].is_ascii_alphanumeric() || bytes[end] == b'_') {
            end += 1;
        }
        if end == start {
            // '$' not followed by a name or brace → literal.
            out.push('$');
            i += 1;
            continue;
        }
        let name = &s[start..end];
        match lookup(name) {
            Some(val) => {
                out.push_str(&val);
                changed = true;
            }
            // Unset → the `$NAME` reference stays literal.
            None => out.push_str(&s[i..end]),
        }
        i = end;
    }
    if changed { Some(out) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_yaml::Value;
    use std::collections::HashMap;

    /// A fixed-table lookup so the expansion rules are tested without touching
    /// the process-global environment (racy under parallel tests).
    fn table(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name: &str| map.get(name).cloned()
    }

    fn expand(s: &str, pairs: &[(&str, &str)]) -> String {
        let lookup = table(pairs);
        expand_str(s, &lookup).unwrap_or_else(|| s.to_string())
    }

    #[test]
    fn set_var_expands_bare_and_braced() {
        let env = &[("FOO", "bar")];
        assert_eq!(expand("$FOO", env), "bar");
        assert_eq!(expand("${FOO}", env), "bar");
        assert_eq!(expand("pre-$FOO-post", env), "pre-bar-post");
        assert_eq!(expand("pre-${FOO}-post", env), "pre-bar-post");
    }

    #[test]
    fn unset_var_stays_literal() {
        // Matches Python os.path.expandvars: an unset reference is left as-is.
        assert_eq!(expand("$MISSING", &[]), "$MISSING");
        assert_eq!(expand("${MISSING}", &[]), "${MISSING}");
        assert_eq!(expand("a/${MISSING}/b", &[]), "a/${MISSING}/b");
    }

    #[test]
    fn dollar_edge_cases() {
        let env = &[("FOO", "bar")];
        // Empty braces, unclosed braces, and a bare '$' are all literal.
        assert_eq!(expand("${}", env), "${}");
        assert_eq!(expand("${UNCLOSED", env), "${UNCLOSED");
        assert_eq!(expand("cost is $ 5", env), "cost is $ 5");
        assert_eq!(expand("100$", env), "100$");
        // '$$FOO' → the first '$' is literal, the second starts a reference.
        assert_eq!(expand("$$FOO", env), "$bar");
        // Adjacent references, name terminated by a non-word char.
        assert_eq!(expand("${FOO}${FOO}", env), "barbar");
        assert_eq!(expand("$FOO/baz", env), "bar/baz");
    }

    #[test]
    fn non_string_scalars_untouched() {
        // A mapping mixing a string value with number / bool / null: only the
        // string expands; the scalars are byte-for-byte identical afterward.
        let mut v: Value =
            serde_yaml::from_str("uri: ${FOO}\nport: 8000\nverbose: true\nempty: null\n").unwrap();
        expand_with(&mut v, &table(&[("FOO", "sqlite:///x.db")]));
        let m = v.as_mapping().unwrap();
        assert_eq!(m["uri"].as_str().unwrap(), "sqlite:///x.db");
        assert_eq!(m["port"].as_u64().unwrap(), 8000);
        assert!(m["verbose"].as_bool().unwrap());
        assert!(m["empty"].is_null());
    }

    #[test]
    fn nested_config_shaped_tree_expands_at_every_depth() {
        // A config-shaped tree: nested mapping + a sequence of mappings.
        let mut v: Value = serde_yaml::from_str(
            "catalog:\n  uri: ${DB}\nallow_origins:\n  - ${ORIGIN}\ntrees:\n  - tree: catalog\n    args:\n      uri: ${DB}\n",
        )
        .unwrap();
        expand_with(
            &mut v,
            &table(&[("DB", "sqlite:///c.db"), ("ORIGIN", "https://a.example")]),
        );
        let m = v.as_mapping().unwrap();
        assert_eq!(m["catalog"]["uri"].as_str().unwrap(), "sqlite:///c.db");
        assert_eq!(m["allow_origins"][0].as_str().unwrap(), "https://a.example");
        assert_eq!(
            m["trees"][0]["args"]["uri"].as_str().unwrap(),
            "sqlite:///c.db"
        );
    }

    #[test]
    fn profile_shaped_tree_expands() {
        // A client-profile-shaped tree: the api_key value is a secret reference.
        let mut v: Value =
            serde_yaml::from_str("my_profile:\n  uri: ${URL}\n  api_key: ${SECRET_KEY}\n").unwrap();
        expand_with(
            &mut v,
            &table(&[("URL", "https://tiled.example"), ("SECRET_KEY", "s3cr3t")]),
        );
        let p = &v.as_mapping().unwrap()["my_profile"];
        assert_eq!(p["uri"].as_str().unwrap(), "https://tiled.example");
        assert_eq!(p["api_key"].as_str().unwrap(), "s3cr3t");
    }

    #[test]
    fn mapping_keys_are_not_expanded() {
        // Only values expand; a `${FOO}` appearing as a key is preserved.
        let mut v: Value = serde_yaml::from_str("${FOO}: ${FOO}\n").unwrap();
        expand_with(&mut v, &table(&[("FOO", "bar")]));
        let m = v.as_mapping().unwrap();
        assert!(
            m.contains_key(Value::String("${FOO}".into())),
            "the key must stay literal"
        );
        assert_eq!(m[Value::String("${FOO}".into())].as_str().unwrap(), "bar");
    }

    #[test]
    fn public_expand_uses_the_real_environment() {
        // Wiring check through the real process env, using an existing variable
        // so nothing is mutated (mutation would race the parallel test binary).
        let path = std::env::var("PATH").expect("PATH is set in the test env");
        let mut v: Value = serde_yaml::from_str("value: ${PATH}\n").unwrap();
        expand_env_vars(&mut v);
        assert_eq!(v["value"].as_str().unwrap(), path);
    }
}
