-- tiled-rs authentication / authorization schema (SQLite).
--
-- Mirrors `tiled.authn_database.orm` with the relations Tiled uses in
-- multi-user mode. JSON columns hold scope/role lists.

CREATE TABLE IF NOT EXISTS principals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL DEFAULT 'user', -- 'user' | 'service'
    time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    time_updated TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE TABLE IF NOT EXISTS identities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id INTEGER NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    -- The provider's stable subject ID (e.g. OIDC `sub`, dummy username).
    sub TEXT NOT NULL,
    latest_login TEXT,
    UNIQUE (provider, sub)
);

CREATE INDEX IF NOT EXISTS idx_identities_principal_id ON identities(principal_id);

CREATE TABLE IF NOT EXISTS api_keys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id INTEGER NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    -- Hash of the actual key (Argon2id over key bytes). The plaintext is
    -- shown to the user only at creation time.
    secret_hash TEXT NOT NULL,
    -- First eight characters of the plaintext, used to identify a key
    -- without revealing it (e.g. `tiled api-key revoke <first_eight>`).
    first_eight TEXT NOT NULL,
    note TEXT,
    -- JSON array of granted scope strings (e.g. ["read:metadata", "read:data"]).
    scopes TEXT NOT NULL DEFAULT '[]',
    -- ISO timestamp; NULL = never expires.
    expiration_time TEXT,
    time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    latest_activity TEXT
);

CREATE INDEX IF NOT EXISTS idx_api_keys_first_eight ON api_keys(first_eight);
CREATE INDEX IF NOT EXISTS idx_api_keys_principal_id ON api_keys(principal_id);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    principal_id INTEGER NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    uuid TEXT NOT NULL UNIQUE,
    -- ISO timestamp.
    time_last_used TEXT,
    expiration_time TEXT NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0,
    -- JSON array of scope strings — narrower than the principal's max
    -- scope set when issued for a specific apikey.
    scopes TEXT NOT NULL DEFAULT '[]',
    time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_sessions_principal_id ON sessions(principal_id);

-- Pending OAuth2 device-code grants. Each row is a (device_code, user_code)
-- pair waiting for a logged-in user to approve.
CREATE TABLE IF NOT EXISTS device_codes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_code TEXT NOT NULL UNIQUE,
    user_code TEXT NOT NULL UNIQUE,
    -- principal_id is set after approval; until then NULL → "pending".
    principal_id INTEGER REFERENCES principals(id) ON DELETE SET NULL,
    expires_at TEXT NOT NULL,
    interval_seconds INTEGER NOT NULL DEFAULT 5,
    last_polled_at TEXT,
    time_created TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);

CREATE INDEX IF NOT EXISTS idx_device_codes_user_code ON device_codes(user_code);
