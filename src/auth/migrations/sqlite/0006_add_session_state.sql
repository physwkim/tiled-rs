-- OBO session state (tiled#1364 EntraAuthenticator). A JSON object persisted
-- per session and embedded verbatim in every access token's `state` claim, so
-- downstream services can read the upstream IdP access/refresh tokens
-- (entra_access_token / entra_refresh_token) for on-behalf-of exchanges.
-- Empty object for non-Entra sessions. Stored as TEXT JSON, mirroring `scopes`.
ALTER TABLE sessions ADD COLUMN state TEXT NOT NULL DEFAULT '{}';
