-- OIDC authorization-code (PKCE browser) flow — pending-state store.
--
-- Server-brokered PKCE state kept between the `/authorize` redirect (created)
-- and the `/callback` completion (consumed, single use). Replaces the former
-- in-memory PendingAuthStore so the flow survives restarts and multi-process /
-- load-balanced deployments where `/callback` may land on a different process
-- than `/authorize` (G6). There is NO Python-tiled equivalent table: Python's
-- browser code flow is a stateless confidential-client exchange with no
-- server-side PKCE; tiled-rs brokers PKCE itself and so must persist this state.
CREATE TABLE IF NOT EXISTS oidc_flow_states (
    -- SHA-256 hash (hex) of the random `state` parameter. The raw `state` is
    -- only ever sent to the browser (and echoed back on the callback), never
    -- stored, so a DB leak cannot forge a callback.
    hashed_state TEXT PRIMARY KEY NOT NULL,
    -- Provider that initiated the flow; selects the token/JWKS config on callback.
    provider TEXT NOT NULL,
    -- PKCE code verifier (RFC 7636 §4.1), presented to the IdP token endpoint.
    code_verifier TEXT NOT NULL,
    -- OIDC nonce expected in the returned id_token (OIDC Core §3.1.3.7 #11).
    nonce TEXT NOT NULL,
    -- Entries past this are rejected; the row is consumed (deleted) on callback
    -- regardless, so a replayed callback finds nothing.
    expiration_time TEXT NOT NULL,
    time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
