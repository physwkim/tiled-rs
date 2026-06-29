//! Typed query operators for the Tiled search API.
//!
//! Re-exports all query types from [`crate::core::queries`]. Pass any [`Query`]
//! to [`ContainerClient::search`](crate::client::ContainerClient::search) to filter
//! search results. The [`Key`] builder covers the most common cases: equality,
//! inequality, and numeric comparisons.
//!
//! # Wire format
//!
//! Each [`Query`] variant encodes to URL parameters of the form
//! `filter[<name>][condition][<field>]=<value>`. The names and field names
//! match Python's `@register(name=...)` decorators and `encode()` methods in
//! `tiled/queries.py`.
//!
//! # Examples
//!
//! ```no_run
//! use crate::client::queries::{FullText, Key, Query, Regex, StructureFamily, StructureFamilyQuery};
//! # use crate::client::ContainerClient;
//! # async fn run(c: ContainerClient) -> crate::client::Result<()> {
//! // Full-text search
//! let items = c.clone().search(Query::FullText(FullText { text: "hello".into() })).keys().await?;
//! // Key equality
//! let items = c.clone().search(Key::new("color").eq("red")).keys().await?;
//! // Numeric comparison
//! let items = c.clone().search(Key::new("temperature").gt(300)).keys().await?;
//! // Structure family
//! let items = c.clone().search(Query::StructureFamily(StructureFamilyQuery {
//!     value: StructureFamily::Array,
//! })).keys().await?;
//! # Ok(()) }
//! ```

pub use crate::core::queries::{
    AccessBlobFilter, Comparison, Contains, Eq, FullText, In, Key, KeyLookup, KeyPresent,
    KeysFilter, Like, NotEq, NotIn, Operator, Query, Regex, SpecsQuery, StructureFamilyQuery,
};
pub use crate::core::structures::StructureFamily;

#[cfg(test)]
mod tests {
    use super::*;

    // Each test names the operator and asserts the exact wire key=value pairs
    // that the tiled server expects, matching Python tiled/queries.py encode().

    #[test]
    fn fulltext_wire_params() {
        let pairs = Query::FullText(FullText {
            text: "hello world".into(),
        })
        .encode();
        assert_eq!(
            pairs,
            vec![(
                "filter[fulltext][condition][text]".to_string(),
                "hello world".to_string()
            )]
        );
    }

    #[test]
    fn eq_wire_string_value() {
        // Python: json.dumps("red") = '"red"' (JSON-quoted string)
        let pairs = Key::new("color").eq("red").encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[eq][condition][key]" && v == "color")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[eq][condition][value]" && v == "\"red\"")
        );
    }

    #[test]
    fn eq_wire_numeric_value() {
        // Python: json.dumps(42) = "42" (no quotes)
        let pairs = Key::new("count").eq(serde_json::json!(42)).encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[eq][condition][value]" && v == "42")
        );
    }

    #[test]
    fn noteq_wire_params() {
        // Python register name: "noteq" (not "not_eq")
        let pairs = Key::new("color").ne("blue").encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[noteq][condition][key]" && v == "color")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[noteq][condition][value]" && v == "\"blue\"")
        );
    }

    #[test]
    fn comparison_gt_wire_params() {
        let pairs = Key::new("temperature").gt(serde_json::json!(300)).encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][operator]" && v == "gt")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][key]" && v == "temperature")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][value]" && v == "300")
        );
    }

    #[test]
    fn comparison_lt_wire_params() {
        let pairs = Key::new("x").lt(serde_json::json!(0)).encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][operator]" && v == "lt")
        );
    }

    #[test]
    fn comparison_le_wire_params() {
        let pairs = Key::new("x").le(serde_json::json!(1.5)).encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][operator]" && v == "le")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][value]" && v == "1.5")
        );
    }

    #[test]
    fn comparison_ge_wire_params() {
        let pairs = Key::new("x").ge(serde_json::json!(10)).encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[comparison][condition][operator]" && v == "ge")
        );
    }

    #[test]
    fn contains_wire_params() {
        let pairs = Query::Contains(Contains {
            key: "detectors".into(),
            value: serde_json::json!("ccd"),
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[contains][condition][key]" && v == "detectors")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[contains][condition][value]" && v == "\"ccd\"")
        );
    }

    #[test]
    fn in_wire_params() {
        let pairs = Query::In(In {
            key: "color".into(),
            value: vec![serde_json::json!("red"), serde_json::json!("blue")],
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[in][condition][key]" && v == "color")
        );
        // Python: json.dumps(["red","blue"]) = '["red","blue"]'
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[in][condition][value]" && v == "[\"red\",\"blue\"]")
        );
    }

    #[test]
    fn notin_wire_params() {
        let pairs = Query::NotIn(NotIn {
            key: "status".into(),
            value: vec![serde_json::json!("draft"), serde_json::json!("archived")],
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[notin][condition][key]" && v == "status")
        );
        assert!(pairs.iter().any(
            |(k, v)| k == "filter[notin][condition][value]" && v == "[\"draft\",\"archived\"]"
        ));
    }

    #[test]
    fn keypresent_exists_true_wire_params() {
        // Python: {"key":…, "exists": True}; Rust emits JSON "true" (lowercase).
        // The server decoder accepts both "true" and "True".
        let pairs = Query::KeyPresent(KeyPresent {
            key: "sample.name".into(),
            exists: true,
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[keypresent][condition][key]" && v == "sample.name")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[keypresent][condition][exists]" && v == "true")
        );
    }

    #[test]
    fn keypresent_exists_false_wire_params() {
        let pairs = Query::KeyPresent(KeyPresent {
            key: "color".into(),
            exists: false,
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[keypresent][condition][exists]" && v == "false")
        );
    }

    #[test]
    fn like_pattern_is_json_quoted() {
        // Python: json.dumps("Ni%") → '"Ni%"' (double-quoted). Rust mirrors this.
        let pairs = Query::Like(Like {
            key: "sample".into(),
            pattern: "Ni%".into(),
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[like][condition][key]" && v == "sample")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[like][condition][pattern]" && v == "\"Ni%\"")
        );
    }

    #[test]
    fn specs_wire_params() {
        let pairs = Query::Specs(SpecsQuery {
            include: vec!["xdi".into()],
            exclude: vec!["draft".into()],
        })
        .encode();
        // Python: json.dumps(["xdi"]) = '["xdi"]'
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[specs][condition][include]" && v == "[\"xdi\"]")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[specs][condition][exclude]" && v == "[\"draft\"]")
        );
    }

    #[test]
    fn specs_empty_exclude_always_emitted() {
        // Python SpecsQuery.encode() always emits both include and exclude;
        // Python decode() requires both. An absent exclude would cause a TypeError.
        let pairs = Query::Specs(SpecsQuery {
            include: vec!["foo".into()],
            exclude: vec![],
        })
        .encode();
        assert!(
            pairs.iter().any(|(k, _)| k.contains("[exclude]")),
            "exclude must always be emitted even when empty"
        );
    }

    #[test]
    fn keys_filter_wire_params() {
        // Python: json.dumps(["a","b"]) = '["a","b"]'
        let pairs = Query::KeysFilter(KeysFilter {
            keys: vec!["a".into(), "b".into()],
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[keys_filter][condition][keys]" && v == "[\"a\",\"b\"]")
        );
    }

    #[test]
    fn regex_case_insensitive_wire_params() {
        let pairs = Query::Regex(Regex {
            key: "sample".into(),
            pattern: r"\d+".into(),
            case_sensitive: false,
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[regex][condition][key]" && v == "sample")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[regex][condition][pattern]" && v == r"\d+")
        );
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[regex][condition][case_sensitive]" && v == "false")
        );
    }

    #[test]
    fn regex_case_sensitive_omits_field() {
        // Python always emits case_sensitive (json.dumps(True) = "true").
        // Rust omits it when true (the server defaults to true when absent).
        // This is a tiled-core encoding choice; the server decodes both correctly.
        let pairs = Query::Regex(Regex {
            key: "sample".into(),
            pattern: r"\d+".into(),
            case_sensitive: true,
        })
        .encode();
        assert!(
            !pairs.iter().any(|(k, _)| k.contains("[case_sensitive]")),
            "Rust omits case_sensitive when true (server defaults to true when absent)"
        );
    }

    #[test]
    fn structure_family_array_wire_params() {
        let pairs = Query::StructureFamily(StructureFamilyQuery {
            value: StructureFamily::Array,
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[structure_family][condition][value]" && v == "array")
        );
    }

    #[test]
    fn structure_family_table_wire_params() {
        let pairs = Query::StructureFamily(StructureFamilyQuery {
            value: StructureFamily::Table,
        })
        .encode();
        assert!(
            pairs
                .iter()
                .any(|(k, v)| k == "filter[structure_family][condition][value]" && v == "table")
        );
    }

    #[test]
    fn access_blob_filter_tags_are_raw_repeated() {
        // Python: tags is a raw list (not json.dumps'd); _queries_to_params
        // appends each element as its own repeated param (container.py:1327-1335).
        let pairs = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: Some("bill".into()),
            tags: vec!["tag1".into(), "tag2".into()],
            include_untagged: false,
        })
        .encode();
        let tag_vals: Vec<&str> = pairs
            .iter()
            .filter(|(k, _)| k == "filter[access_blob_filter][condition][tags]")
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(tag_vals, vec!["tag1", "tag2"], "one raw param per tag");
        assert!(
            !tag_vals.iter().any(|v| v.contains('[')),
            "tags must not be json-stringified"
        );
    }

    #[test]
    fn access_blob_filter_user_id_emitted() {
        let pairs = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: Some("amanda".into()),
            tags: vec![],
            include_untagged: false,
        })
        .encode();
        assert!(
            pairs.iter().any(
                |(k, v)| k == "filter[access_blob_filter][condition][user_id]" && v == "amanda"
            )
        );
    }

    #[test]
    fn access_blob_filter_no_user_id_omits_field() {
        let pairs = Query::AccessBlobFilter(AccessBlobFilter {
            user_id: None,
            tags: vec!["public".into()],
            include_untagged: false,
        })
        .encode();
        assert!(
            !pairs.iter().any(|(k, _)| k.contains("[user_id]")),
            "absent user_id must not emit a param"
        );
    }

    #[test]
    fn key_builder_all_operators_produce_correct_query_names() {
        assert_eq!(
            Key::new("x").lt(serde_json::json!(0)).query_name(),
            "comparison"
        );
        assert_eq!(
            Key::new("x").gt(serde_json::json!(0)).query_name(),
            "comparison"
        );
        assert_eq!(
            Key::new("x").le(serde_json::json!(0)).query_name(),
            "comparison"
        );
        assert_eq!(
            Key::new("x").ge(serde_json::json!(0)).query_name(),
            "comparison"
        );
        assert_eq!(Key::new("x").eq(serde_json::json!(0)).query_name(), "eq");
        assert_eq!(Key::new("x").ne(serde_json::json!(0)).query_name(), "noteq");
    }
}
