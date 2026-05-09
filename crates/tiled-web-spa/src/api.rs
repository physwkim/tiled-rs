//! Thin client wrappers around the tiled-rs HTTP API.
//!
//! Uses gloo-net's fetch instead of pulling in the full tiled-client
//! crate (which has heavier dependencies). The wire format matches —
//! we serialise/deserialise straight against the JSON the server
//! produces.

use gloo_net::http::Request;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AboutResponse {
    pub api_version: u32,
    pub library_version: String,
    pub queries: Vec<String>,
    #[serde(default)]
    pub authentication: serde_json::Value,
}

pub async fn fetch_about() -> Result<AboutResponse, String> {
    let resp = Request::get("/api/v1/")
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceEnvelope {
    pub data: ResourceData,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceData {
    pub id: String,
    pub attributes: ResourceAttributes,
    #[serde(default)]
    pub links: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResourceAttributes {
    pub structure_family: Option<String>,
    #[serde(default)]
    pub specs: Vec<serde_json::Value>,
    pub metadata: Option<serde_json::Value>,
    pub structure: Option<serde_json::Value>,
    #[serde(default)]
    pub ancestors: Vec<String>,
}

pub async fn fetch_metadata(path: &str) -> Result<ResourceEnvelope, String> {
    let url = if path.is_empty() {
        "/api/v1/metadata/".to_string()
    } else {
        format!("/api/v1/metadata/{path}")
    };
    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchEnvelope {
    pub data: Vec<ResourceData>,
    #[serde(default)]
    pub meta: serde_json::Value,
}

pub async fn fetch_children(path: &str) -> Result<SearchEnvelope, String> {
    let url = if path.is_empty() {
        "/api/v1/search/?page[limit]=100".to_string()
    } else {
        format!("/api/v1/search/{path}?page[limit]=100")
    };
    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    resp.json().await.map_err(|e| e.to_string())
}
