-- IdP-brokered OAuth2 device-code flow.
--
-- Distinct from `device_codes` (tiled's NATIVE RFC 8628 grant against local
-- principals). Each row here is a pending device-login that is fulfilled when
-- the user completes an EXTERNAL OIDC authorization-code flow in a browser and
-- submits their user_code. Mirrors Python tiled's `pending_sessions` table
-- (authn_database/orm.py:229 PendingSession).
CREATE TABLE IF NOT EXISTS pending_sessions (
    -- SHA-256 hash (hex) of the device_code. The raw device_code is only ever
    -- returned to the client, never stored, so a DB leak cannot replay it.
    hashed_device_code TEXT PRIMARY KEY NOT NULL,
    user_code TEXT NOT NULL,
    expiration_time TIMESTAMPTZ NOT NULL,
    -- NULL until the browser-side OIDC login binds a real session; the token
    -- poll returns `authorization_pending` while this is NULL. CASCADE so a
    -- deleted session never leaves a dangling pending row.
    session_id BIGINT REFERENCES sessions(id) ON DELETE CASCADE,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pending_sessions_user_code ON pending_sessions(user_code);
