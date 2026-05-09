//! Wire types for the SPA auth flow.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthTokens {
    pub access_token: String,
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserIdentity {
    pub id: String,
    pub provider: String,
}

/// One entry from `GET /api/v1/`'s `authentication.providers`. We
/// duplicate the fields we care about — `provider`, `mode`, and the
/// `auth_endpoint` link — and keep everything else as a
/// `serde_json::Value` so unknown fields don't break deserialisation.
#[derive(Debug, Clone, Deserialize)]
pub struct ProviderInfo {
    pub provider: String,
    pub mode: String,
    pub links: ProviderLinks,
    #[serde(default)]
    pub confirmation_message: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderLinks {
    pub auth_endpoint: String,
}

/// Server's `/api/v1/auth/{provider}/login` response shape.
#[derive(Debug, Clone, Deserialize)]
pub struct LoginResponse {
    pub access_token: String,
    pub refresh_token: String,
    #[serde(default)]
    pub identity: Option<UserIdentity>,
}

/// Server's `/api/v1/auth/refresh` response shape — refresh-grant emits
/// a new access token but reuses the existing refresh token, so this
/// path only carries `access_token`.
#[derive(Debug, Clone, Deserialize)]
pub struct RefreshResponse {
    pub access_token: String,
}
