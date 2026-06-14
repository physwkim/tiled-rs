//! HTML serializer — minimal browseable index for `container` nodes.
//!
//! Renders a list of children as `<a href>` entries so an operator can
//! navigate through `/api/v1/metadata/...` in a regular browser when
//! debugging. Input is the JSON-encoded entries response (the same
//! Vec<Resource> the search endpoint emits).

use bytes::Bytes;

use tiled_core::structures::StructureFamily;

use crate::registry::{SerializationRegistry, SerializerFn};

pub const TEXT_HTML: &str = "text/html";

pub fn register_html_serializer(reg: &SerializationRegistry) {
    reg.register(StructureFamily::Container, TEXT_HTML, html_serializer());
    reg.register_alias(".html", TEXT_HTML);
}

fn html_serializer() -> SerializerFn {
    Box::new(
        |data, meta| -> Result<Bytes, crate::registry::SerializeError> {
            let value: serde_json::Value =
                serde_json::from_slice(data).map_err(|e| format!("decode input: {e}"))?;
            let entries = value.as_array().ok_or("html input must be a list")?;
            let path = meta.get("path").and_then(|v| v.as_str()).unwrap_or("");
            let mut html = String::new();
            html.push_str("<!doctype html><html><head><meta charset=\"utf-8\">");
            html.push_str("<title>Tiled — ");
            html.push_str(&html_escape(path));
            html.push_str("</title></head><body>");
            html.push_str("<h1>");
            html.push_str(&html_escape(path));
            html.push_str("</h1><ul>");
            for entry in entries {
                let id = entry.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                let family = entry
                    .get("attributes")
                    .and_then(|a| a.get("structure_family"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("?");
                let self_link = entry
                    .get("links")
                    .and_then(|l| l.get("self"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                html.push_str("<li><a href=\"");
                html.push_str(&html_escape(self_link));
                html.push_str("\">");
                html.push_str(&html_escape(id));
                html.push_str("</a> <em>(");
                html.push_str(&html_escape(family));
                html.push_str(")</em></li>");
            }
            html.push_str("</ul></body></html>");
            Ok(Bytes::from(html))
        },
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
