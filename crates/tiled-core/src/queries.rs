//! Query types for searching and filtering catalog entries.
//!
//! Corresponds to `tiled/queries.py`.
//!
//! These query objects are encoding-agnostic: they describe *what* to search for,
//! not *how* the search is executed. The catalog adapter translates them to SQL.
//!
//! Queries arrive as URL params: `filter[fulltext][condition][text]=hello`.
//! The `decode_query_filters` function parses these into `Query` variants.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::structures::StructureFamily;

/// Comparison operators for ordered queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operator {
    Lt,
    Gt,
    Le,
    Ge,
}

impl std::fmt::Display for Operator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Lt => write!(f, "lt"),
            Self::Gt => write!(f, "gt"),
            Self::Le => write!(f, "le"),
            Self::Ge => write!(f, "ge"),
        }
    }
}

impl std::str::FromStr for Operator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "lt" => Ok(Self::Lt),
            "gt" => Ok(Self::Gt),
            "le" => Ok(Self::Le),
            "ge" => Ok(Self::Ge),
            _ => Err(format!("unknown operator: {s}")),
        }
    }
}

/// Full-text search across all metadata values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FullText {
    pub text: String,
}

/// Match a specific entry by key (for item lookup within search results).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyLookup {
    pub key: String,
}

/// Filter entries to only those matching one of the specified keys.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeysFilter {
    pub keys: Vec<String>,
}

/// Match a key's value against a regular expression.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Regex {
    pub key: String,
    pub pattern: String,
    #[serde(default = "default_true")]
    pub case_sensitive: bool,
}

fn default_true() -> bool {
    true
}

/// Query equality of a metadata key's value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Eq {
    pub key: String,
    pub value: serde_json::Value,
}

/// Query inequality of a metadata key's value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotEq {
    pub key: String,
    pub value: serde_json::Value,
}

/// Binary comparison (gt, lt, ge, le) of a metadata key's value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Comparison {
    pub operator: Operator,
    pub key: String,
    pub value: serde_json::Value,
}

/// Check if a key's value contains a specified value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Contains {
    pub key: String,
    pub value: serde_json::Value,
}

/// Check if a key's value is present in a list of values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct In {
    pub key: String,
    pub value: Vec<serde_json::Value>,
}

/// Check if a key's value is NOT in a list of values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NotIn {
    pub key: String,
    pub value: Vec<serde_json::Value>,
}

/// Check if a metadata key exists (or does not exist).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyPresent {
    pub key: String,
    #[serde(default = "default_true")]
    pub exists: bool,
}

/// SQL LIKE pattern matching on a metadata key's value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Like {
    pub key: String,
    pub pattern: String,
}

/// Match specs list: must contain all `include` and none of `exclude`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpecsQuery {
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

/// Filter by access_blob — user_id and/or tags.
///
/// A row matches when ANY of the following is true:
/// * `user_id` is `Some(id)` and `access_blob.user == id`
/// * `access_blob.tags` contains any tag in `tags`
/// * `include_untagged` is `true` and `access_blob.tags` is absent/empty AND
///   `access_blob` has no `user` key (genuinely public)
///
/// When all three conditions are vacuously false the row is excluded.
/// (Empty `tags`, no `user_id`, and `include_untagged = false` → deny all.)
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct AccessBlobFilter {
    pub user_id: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    /// When `true`, rows whose `access_blob.tags` is absent or empty AND that
    /// carry no `user` key are treated as "public" and match for everyone. A
    /// user-owned blob `{"user": id}` is excluded from this arm — it matches
    /// only its owner via `user_id`, so the flag never leaks owned rows.
    #[serde(default)]
    pub include_untagged: bool,
}

/// Filter by structure family.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StructureFamilyQuery {
    pub value: StructureFamily,
}

/// Any supported query variant.
///
/// Note: queries never arrive as JSON bodies — they come as URL parameters.
/// The tagged serde here is for internal use only.
#[derive(Debug, Clone, PartialEq)]
pub enum Query {
    FullText(FullText),
    Lookup(KeyLookup),
    KeysFilter(KeysFilter),
    Regex(Regex),
    Eq(Eq),
    NotEq(NotEq),
    Comparison(Comparison),
    Contains(Contains),
    In(In),
    NotIn(NotIn),
    KeyPresent(KeyPresent),
    Like(Like),
    Specs(SpecsQuery),
    AccessBlobFilter(AccessBlobFilter),
    StructureFamily(StructureFamilyQuery),
}

impl Query {
    /// Returns the registry name matching Python's `@register(name=...)` decorators.
    pub fn query_name(&self) -> &'static str {
        match self {
            Self::FullText(_) => "fulltext",
            Self::Lookup(_) => "lookup",
            Self::KeysFilter(_) => "keys_filter",
            Self::Regex(_) => "regex",
            Self::Eq(_) => "eq",
            Self::NotEq(_) => "noteq",
            Self::Comparison(_) => "comparison",
            Self::Contains(_) => "contains",
            Self::In(_) => "in",
            Self::NotIn(_) => "notin",
            Self::KeyPresent(_) => "keypresent",
            Self::Like(_) => "like",
            Self::Specs(_) => "specs",
            Self::AccessBlobFilter(_) => "access_blob_filter",
            Self::StructureFamily(_) => "structure_family",
        }
    }

    /// Returns the Python class name for this query variant.
    ///
    /// This is the identifier Python tiled embeds in its `UnsupportedQueryType`
    /// 400 detail (`The query type {name!r} is not supported on this node.`,
    /// tiled/server/app.py:355-365; the name comes from `class_.__name__`,
    /// tiled/query_registration.py:127). The Rust enum variant names diverge
    /// from the Python class names for three variants, so this is a distinct
    /// mapping from [`Query::query_name`] (which returns the URL registry name).
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::FullText(_) => "FullText",
            Self::Lookup(_) => "KeyLookup",
            Self::KeysFilter(_) => "KeysFilter",
            Self::Regex(_) => "Regex",
            Self::Eq(_) => "Eq",
            Self::NotEq(_) => "NotEq",
            Self::Comparison(_) => "Comparison",
            Self::Contains(_) => "Contains",
            Self::In(_) => "In",
            Self::NotIn(_) => "NotIn",
            Self::KeyPresent(_) => "KeyPresent",
            Self::Like(_) => "Like",
            Self::Specs(_) => "SpecsQuery",
            Self::AccessBlobFilter(_) => "AccessBlobFilter",
            Self::StructureFamily(_) => "StructureFamilyQuery",
        }
    }

    /// All registered query type names.
    pub fn all_query_names() -> Vec<&'static str> {
        vec![
            "fulltext",
            "lookup",
            "keys_filter",
            "regex",
            "eq",
            "noteq",
            "comparison",
            "contains",
            "in",
            "notin",
            "keypresent",
            "like",
            "specs",
            "access_blob_filter",
            "structure_family",
        ]
    }

    /// Encode this query as URL parameter key-value pairs.
    ///
    /// Returns pairs like `("filter[eq][condition][key]", "color")`,
    /// `("filter[eq][condition][value]", "\"red\"")`.
    ///
    /// Multiple queries of the same type (e.g. two `Comparison` bounds for a
    /// range) must be encoded together: concatenate the `Vec`s from each call
    /// and append every pair with `url.query_pairs_mut().append_pair`. Using a
    /// `HashMap` to build the URL would silently drop repeated keys.
    pub fn encode(&self) -> Vec<(String, String)> {
        let name = self.query_name();
        let prefix = format!("filter[{name}][condition]");
        let mut params: Vec<(String, String)> = Vec::new();
        match self {
            Self::FullText(q) => {
                params.push((format!("{prefix}[text]"), q.text.clone()));
            }
            Self::Lookup(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
            }
            Self::KeysFilter(q) => {
                let v = serde_json::to_string(&q.keys).unwrap_or_default();
                params.push((format!("{prefix}[keys]"), v));
            }
            Self::Regex(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((format!("{prefix}[pattern]"), q.pattern.clone()));
                if !q.case_sensitive {
                    params.push((format!("{prefix}[case_sensitive]"), "false".into()));
                }
            }
            Self::Eq(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                ));
            }
            Self::NotEq(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                ));
            }
            Self::Comparison(q) => {
                params.push((format!("{prefix}[operator]"), q.operator.to_string()));
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                ));
            }
            Self::Contains(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                ));
            }
            Self::In(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                ));
            }
            Self::NotIn(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                ));
            }
            Self::KeyPresent(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[exists]"),
                    serde_json::to_string(&q.exists).unwrap_or_default(),
                ));
            }
            Self::Like(q) => {
                params.push((format!("{prefix}[key]"), q.key.clone()));
                params.push((
                    format!("{prefix}[pattern]"),
                    serde_json::to_string(&q.pattern).unwrap_or_default(),
                ));
            }
            Self::Specs(q) => {
                params.push((
                    format!("{prefix}[include]"),
                    serde_json::to_string(&q.include).unwrap_or_default(),
                ));
                params.push((
                    format!("{prefix}[exclude]"),
                    serde_json::to_string(&q.exclude).unwrap_or_default(),
                ));
            }
            Self::AccessBlobFilter(q) => {
                if let Some(ref uid) = q.user_id {
                    params.push((format!("{prefix}[user_id]"), uid.clone()));
                }
                if !q.tags.is_empty() {
                    params.push((
                        format!("{prefix}[tags]"),
                        serde_json::to_string(&q.tags).unwrap_or_default(),
                    ));
                }
            }
            Self::StructureFamily(q) => {
                params.push((format!("{prefix}[value]"), q.value.to_string()));
            }
        }
        params
    }
}

/// A query variant a node cannot evaluate.
///
/// Returned by [`crate::adapters::ContainerAdapter::search`] when a query type
/// is not supported by that adapter's search path. Mirrors Python tiled's
/// `UnsupportedQueryType` (tiled/utils.py:601), which the server turns into
/// HTTP 400 with detail `The query type {name!r} is not supported on this
/// node.` (tiled/server/app.py:355-365). The wrapped string is the query's
/// Python class name (see [`Query::type_name`]), so the 400 detail matches
/// upstream byte-for-byte. `Display` renders that full detail string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedQuery(pub String);

impl std::fmt::Display for UnsupportedQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "The query type '{}' is not supported on this node.",
            self.0
        )
    }
}

impl std::error::Error for UnsupportedQuery {}

/// Regex pattern for extracting filter parameters from URL query string.
/// Matches `filter[<name>][condition][<field>]`.
static FILTER_PARAM_PATTERN: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"^filter\[([^\]]+)\]\[condition\]\[([^\]]+)\]$").unwrap()
});

/// Decode query filter parameters from URL query pairs.
///
/// Parses pairs like `("filter[eq][condition][key]", "color")` into `Query` variants.
///
/// Repeated keys for the same filter type are collected into per-field lists and
/// reconstructed via positional zip (index 0 → first query, index 1 → second, …).
/// This matches Python's wire format where `params[key].append(value)` builds
/// per-field lists and the server iterates `i = 0, 1, …` until `IndexError`.
pub fn decode_query_filters(params: &[(String, String)]) -> Vec<Query> {
    // name → field → ordered list of values (one entry per query of that type)
    let mut groups: HashMap<String, HashMap<String, Vec<String>>> = HashMap::new();
    for (key, value) in params {
        if let Some(caps) = FILTER_PARAM_PATTERN.captures(key) {
            groups
                .entry(caps[1].to_string())
                .or_default()
                .entry(caps[2].to_string())
                .or_default()
                .push(value.clone());
        }
    }

    let mut queries = Vec::new();
    for (name, field_lists) in &groups {
        // Number of queries of this type = length of the shortest per-field list.
        let n = field_lists.values().map(|v| v.len()).min().unwrap_or(0);
        for i in 0..n {
            let fields: HashMap<String, String> = field_lists
                .iter()
                .map(|(f, vs)| (f.clone(), vs[i].clone()))
                .collect();
            if let Some(q) = decode_single_query(name, &fields) {
                queries.push(q);
            }
        }
    }
    queries
}

fn decode_single_query(name: &str, fields: &HashMap<String, String>) -> Option<Query> {
    match name {
        "fulltext" => {
            let text = fields.get("text")?.clone();
            Some(Query::FullText(FullText { text }))
        }
        "lookup" => {
            let key = fields.get("key")?.clone();
            Some(Query::Lookup(KeyLookup { key }))
        }
        "keys_filter" => {
            let keys_str = fields.get("keys")?;
            let keys: Vec<String> = serde_json::from_str(keys_str).ok()?;
            Some(Query::KeysFilter(KeysFilter { keys }))
        }
        "regex" => {
            let key = fields.get("key")?.clone();
            let pattern = fields.get("pattern")?.clone();
            let case_sensitive = fields
                .get("case_sensitive")
                .map(|v| v != "false")
                .unwrap_or(true);
            Some(Query::Regex(Regex {
                key,
                pattern,
                case_sensitive,
            }))
        }
        "eq" => {
            let key = fields.get("key")?.clone();
            let value: serde_json::Value = serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::Eq(Eq { key, value }))
        }
        "noteq" => {
            let key = fields.get("key")?.clone();
            let value: serde_json::Value = serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::NotEq(NotEq { key, value }))
        }
        "comparison" => {
            let operator: Operator = fields.get("operator")?.parse().ok()?;
            let key = fields.get("key")?.clone();
            let value: serde_json::Value = serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::Comparison(Comparison {
                operator,
                key,
                value,
            }))
        }
        "contains" => {
            let key = fields.get("key")?.clone();
            let value: serde_json::Value = serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::Contains(Contains { key, value }))
        }
        "in" => {
            let key = fields.get("key")?.clone();
            let value: Vec<serde_json::Value> = serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::In(In { key, value }))
        }
        "notin" => {
            let key = fields.get("key")?.clone();
            let value: Vec<serde_json::Value> = serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::NotIn(NotIn { key, value }))
        }
        "keypresent" => {
            let key = fields.get("key")?.clone();
            let exists = fields
                .get("exists")
                .map(|v| {
                    // Accept JSON "true"/"false" (Rust encode) and Python "True"/"False".
                    serde_json::from_str::<bool>(v).unwrap_or_else(|_| v.to_lowercase() != "false")
                })
                .unwrap_or(true);
            Some(Query::KeyPresent(KeyPresent { key, exists }))
        }
        "like" => {
            let key = fields.get("key")?.clone();
            let pattern: String = serde_json::from_str(fields.get("pattern")?).ok()?;
            Some(Query::Like(Like { key, pattern }))
        }
        "specs" => {
            let include: Vec<String> = serde_json::from_str(fields.get("include")?).ok()?;
            let exclude: Vec<String> = fields
                .get("exclude")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            Some(Query::Specs(SpecsQuery { include, exclude }))
        }
        "access_blob_filter" => {
            let user_id = fields.get("user_id").cloned();
            let tags: Vec<String> = fields
                .get("tags")
                .and_then(|s| serde_json::from_str(s).ok())
                .unwrap_or_default();
            Some(Query::AccessBlobFilter(AccessBlobFilter {
                user_id,
                tags,
                include_untagged: false,
            }))
        }
        "structure_family" => {
            let value: StructureFamily = fields.get("value")?.parse().ok()?;
            Some(Query::StructureFamily(StructureFamilyQuery { value }))
        }
        _ => None,
    }
}

/// Builder for metadata key queries (mirrors Python `Key` class).
///
/// ```
/// use tiled_core::queries::{Key, Query};
///
/// let q = Key::new("color").eq("red");
/// let q = Key::new("temperature").gt(300);
/// ```
pub struct Key {
    key: String,
}

impl Key {
    pub fn new(key: impl Into<String>) -> Self {
        Self { key: key.into() }
    }

    pub fn eq(self, value: impl Into<serde_json::Value>) -> Query {
        Query::Eq(Eq {
            key: self.key,
            value: value.into(),
        })
    }

    pub fn ne(self, value: impl Into<serde_json::Value>) -> Query {
        Query::NotEq(NotEq {
            key: self.key,
            value: value.into(),
        })
    }

    pub fn lt(self, value: impl Into<serde_json::Value>) -> Query {
        Query::Comparison(Comparison {
            operator: Operator::Lt,
            key: self.key,
            value: value.into(),
        })
    }

    pub fn gt(self, value: impl Into<serde_json::Value>) -> Query {
        Query::Comparison(Comparison {
            operator: Operator::Gt,
            key: self.key,
            value: value.into(),
        })
    }

    pub fn le(self, value: impl Into<serde_json::Value>) -> Query {
        Query::Comparison(Comparison {
            operator: Operator::Le,
            key: self.key,
            value: value.into(),
        })
    }

    pub fn ge(self, value: impl Into<serde_json::Value>) -> Query {
        Query::Comparison(Comparison {
            operator: Operator::Ge,
            key: self.key,
            value: value.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_builder() {
        let q = Key::new("color").eq("red");
        match q {
            Query::Eq(eq) => {
                assert_eq!(eq.key, "color");
                assert_eq!(eq.value, serde_json::json!("red"));
            }
            _ => panic!("Expected Eq query"),
        }
    }

    #[test]
    fn test_comparison_query() {
        let q = Key::new("temperature").gt(300);
        match q {
            Query::Comparison(c) => {
                assert_eq!(c.operator, Operator::Gt);
                assert_eq!(c.key, "temperature");
                assert_eq!(c.value, serde_json::json!(300));
            }
            _ => panic!("Expected Comparison query"),
        }
    }

    #[test]
    fn test_query_names() {
        let q = Query::FullText(FullText {
            text: "hello".into(),
        });
        assert_eq!(q.query_name(), "fulltext");

        let q = Key::new("x").eq(1);
        assert_eq!(q.query_name(), "eq");
    }

    #[test]
    fn test_encode_decode_roundtrip_eq() {
        let q = Key::new("color").eq("red");
        let pairs = q.encode();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].query_name(), "eq");
        match &decoded[0] {
            Query::Eq(eq) => {
                assert_eq!(eq.key, "color");
                assert_eq!(eq.value, serde_json::json!("red"));
            }
            _ => panic!("Expected Eq"),
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_fulltext() {
        let q = Query::FullText(FullText {
            text: "hello world".into(),
        });
        let pairs = q.encode();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::FullText(ft) => assert_eq!(ft.text, "hello world"),
            _ => panic!("Expected FullText"),
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_comparison() {
        let q = Key::new("temperature").gt(300);
        let pairs = q.encode();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::Comparison(c) => {
                assert_eq!(c.operator, Operator::Gt);
                assert_eq!(c.key, "temperature");
                assert_eq!(c.value, serde_json::json!(300));
            }
            _ => panic!("Expected Comparison"),
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_specs() {
        let q = Query::Specs(SpecsQuery {
            include: vec!["xdi".into(), "xas".into()],
            exclude: vec!["draft".into()],
        });
        let pairs = q.encode();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::Specs(s) => {
                assert_eq!(s.include, vec!["xdi", "xas"]);
                assert_eq!(s.exclude, vec!["draft"]);
            }
            _ => panic!("Expected Specs"),
        }
    }

    #[test]
    fn test_encode_decode_roundtrip_structure_family() {
        let q = Query::StructureFamily(StructureFamilyQuery {
            value: StructureFamily::Array,
        });
        let pairs = q.encode();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::StructureFamily(sf) => {
                assert_eq!(sf.value, StructureFamily::Array);
            }
            _ => panic!("Expected StructureFamily"),
        }
    }

    /// H4: Two Comparison queries of the same type must both survive encode+decode.
    /// A gt lower bound and an lt upper bound form a range; previously only one
    /// survived because encode() returned HashMap (duplicate keys) and decode()
    /// grouped per-name into a single HashMap (last value wins).
    #[test]
    fn test_encode_decode_roundtrip_two_comparisons_range() {
        let gt = Key::new("temperature").gt(300);
        let lt = Key::new("temperature").lt(400);
        // Concatenate both encodings — same keys appear twice.
        let mut pairs = gt.encode();
        pairs.extend(lt.encode());

        let decoded = decode_query_filters(&pairs);
        assert_eq!(
            decoded.len(),
            2,
            "both range bounds must survive round-trip"
        );

        let mut ops: Vec<Operator> = decoded
            .iter()
            .filter_map(|q| {
                if let Query::Comparison(c) = q {
                    Some(c.operator)
                } else {
                    None
                }
            })
            .collect();
        ops.sort_by_key(|o| o.to_string());
        assert_eq!(ops, vec![Operator::Gt, Operator::Lt]);
    }

    /// H2: KeyPresent with exists=true must always emit the exists field.
    /// Python KeyPresent.encode() always returns {"key":…, "exists": self.exists}
    /// (queries.py:402). Python decode() requires exists as a keyword arg (:406).
    /// Previously Rust omitted exists when true, so the Python server would crash
    /// with a TypeError when decoding a `KeyPresent(exists=true)` query.
    #[test]
    fn test_encode_decode_keypresent_exists_true() {
        let q = Query::KeyPresent(KeyPresent {
            key: "x".into(),
            exists: true,
        });
        let pairs = q.encode();
        assert!(
            pairs.iter().any(|(k, _)| k.contains("[exists]")),
            "exists field must be emitted even when true"
        );
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::KeyPresent(kp) => {
                assert_eq!(kp.key, "x");
                assert!(kp.exists);
            }
            _ => panic!("Expected KeyPresent"),
        }
    }

    /// H2: KeyPresent with exists=false must also round-trip.
    #[test]
    fn test_encode_decode_keypresent_exists_false() {
        let q = Query::KeyPresent(KeyPresent {
            key: "sample.name".into(),
            exists: false,
        });
        let pairs = q.encode();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::KeyPresent(kp) => {
                assert_eq!(kp.key, "sample.name");
                assert!(!kp.exists);
            }
            _ => panic!("Expected KeyPresent"),
        }
    }

    /// H1: Like.pattern must be JSON-encoded (json.dumps parity with queries.py:441-442).
    /// Previously emitted raw — a pattern like `Ni%` would round-trip only by
    /// accident; the Python server's decode calls json.loads so it expects quotes.
    #[test]
    fn test_encode_decode_like_pattern() {
        let q = Query::Like(Like {
            key: "sample".into(),
            pattern: "Ni%".into(),
        });
        let pairs = q.encode();
        let pattern_val = pairs
            .iter()
            .find(|(k, _)| k.contains("[pattern]"))
            .map(|(_, v)| v.as_str())
            .expect("pattern field must be present");
        assert_eq!(
            pattern_val, "\"Ni%\"",
            "pattern must be JSON-quoted (json.dumps parity)"
        );
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::Like(like) => {
                assert_eq!(like.key, "sample");
                assert_eq!(like.pattern, "Ni%");
            }
            _ => panic!("Expected Like"),
        }
    }

    /// H3: SpecsQuery with empty exclude must always emit the exclude field.
    /// Python SpecsQuery.encode() always returns both include and exclude
    /// (queries.py:483-486). Python SpecsQuery.decode() requires both as keyword
    /// args with no defaults (queries.py:490). Previously Rust omitted exclude
    /// when empty, so the Python server would crash with a TypeError on any
    /// SpecsQuery(exclude=[]) query from a Rust client.
    #[test]
    fn test_encode_decode_specs_empty_exclude() {
        let q = Query::Specs(SpecsQuery {
            include: vec!["foo".into()],
            exclude: vec![],
        });
        let pairs = q.encode();
        assert!(
            pairs.iter().any(|(k, _)| k.contains("[exclude]")),
            "exclude field must always be emitted"
        );
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::Specs(s) => {
                assert_eq!(s.include, vec!["foo"]);
                assert!(s.exclude.is_empty());
            }
            _ => panic!("Expected Specs"),
        }
    }
}
