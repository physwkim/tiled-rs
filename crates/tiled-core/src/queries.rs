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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccessBlobFilter {
    pub user_id: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
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
    pub fn encode(&self) -> HashMap<String, String> {
        let name = self.query_name();
        let prefix = format!("filter[{name}][condition]");
        let mut params = HashMap::new();
        match self {
            Self::FullText(q) => {
                params.insert(format!("{prefix}[text]"), q.text.clone());
            }
            Self::Lookup(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
            }
            Self::KeysFilter(q) => {
                let v = serde_json::to_string(&q.keys).unwrap_or_default();
                params.insert(format!("{prefix}[keys]"), v);
            }
            Self::Regex(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(format!("{prefix}[pattern]"), q.pattern.clone());
                if !q.case_sensitive {
                    params.insert(format!("{prefix}[case_sensitive]"), "false".into());
                }
            }
            Self::Eq(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                );
            }
            Self::NotEq(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                );
            }
            Self::Comparison(q) => {
                params.insert(format!("{prefix}[operator]"), q.operator.to_string());
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                );
            }
            Self::Contains(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                );
            }
            Self::In(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                );
            }
            Self::NotIn(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(
                    format!("{prefix}[value]"),
                    serde_json::to_string(&q.value).unwrap_or_default(),
                );
            }
            Self::KeyPresent(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                if !q.exists {
                    params.insert(format!("{prefix}[exists]"), "false".into());
                }
            }
            Self::Like(q) => {
                params.insert(format!("{prefix}[key]"), q.key.clone());
                params.insert(format!("{prefix}[pattern]"), q.pattern.clone());
            }
            Self::Specs(q) => {
                params.insert(
                    format!("{prefix}[include]"),
                    serde_json::to_string(&q.include).unwrap_or_default(),
                );
                if !q.exclude.is_empty() {
                    params.insert(
                        format!("{prefix}[exclude]"),
                        serde_json::to_string(&q.exclude).unwrap_or_default(),
                    );
                }
            }
            Self::AccessBlobFilter(q) => {
                if let Some(ref uid) = q.user_id {
                    params.insert(format!("{prefix}[user_id]"), uid.clone());
                }
                if !q.tags.is_empty() {
                    params.insert(
                        format!("{prefix}[tags]"),
                        serde_json::to_string(&q.tags).unwrap_or_default(),
                    );
                }
            }
            Self::StructureFamily(q) => {
                params.insert(format!("{prefix}[value]"), q.value.to_string());
            }
        }
        params
    }
}

/// Regex pattern for extracting filter parameters from URL query string.
/// Matches `filter[<name>][condition][<field>]`.
static FILTER_PARAM_PATTERN: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"^filter\[([^\]]+)\]\[condition\]\[([^\]]+)\]$").unwrap()
});

/// Decode query filter parameters from URL query pairs.
///
/// Parses pairs like `("filter[eq][condition][key]", "color")` into `Query` variants.
pub fn decode_query_filters(params: &[(String, String)]) -> Vec<Query> {
    let mut groups: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (key, value) in params {
        if let Some(caps) = FILTER_PARAM_PATTERN.captures(key) {
            groups
                .entry(caps[1].to_string())
                .or_default()
                .insert(caps[2].to_string(), value.clone());
        }
    }

    groups
        .iter()
        .filter_map(|(name, fields)| decode_single_query(name, fields))
        .collect()
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
            let value: Vec<serde_json::Value> =
                serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::In(In { key, value }))
        }
        "notin" => {
            let key = fields.get("key")?.clone();
            let value: Vec<serde_json::Value> =
                serde_json::from_str(fields.get("value")?).ok()?;
            Some(Query::NotIn(NotIn { key, value }))
        }
        "keypresent" => {
            let key = fields.get("key")?.clone();
            let exists = fields
                .get("exists")
                .map(|v| v != "false")
                .unwrap_or(true);
            Some(Query::KeyPresent(KeyPresent { key, exists }))
        }
        "like" => {
            let key = fields.get("key")?.clone();
            let pattern = fields.get("pattern")?.clone();
            Some(Query::Like(Like { key, pattern }))
        }
        "specs" => {
            let include: Vec<String> =
                serde_json::from_str(fields.get("include")?).ok()?;
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
            Some(Query::AccessBlobFilter(AccessBlobFilter { user_id, tags }))
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
        let params = q.encode();
        let pairs: Vec<(String, String)> = params.into_iter().collect();
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
        let params = q.encode();
        let pairs: Vec<(String, String)> = params.into_iter().collect();
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
        let params = q.encode();
        let pairs: Vec<(String, String)> = params.into_iter().collect();
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
        let params = q.encode();
        let pairs: Vec<(String, String)> = params.into_iter().collect();
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
        let params = q.encode();
        let pairs: Vec<(String, String)> = params.into_iter().collect();
        let decoded = decode_query_filters(&pairs);
        assert_eq!(decoded.len(), 1);
        match &decoded[0] {
            Query::StructureFamily(sf) => {
                assert_eq!(sf.value, StructureFamily::Array);
            }
            _ => panic!("Expected StructureFamily"),
        }
    }
}
