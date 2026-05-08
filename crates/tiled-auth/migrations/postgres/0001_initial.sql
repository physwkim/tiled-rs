-- tiled-rs auth schema (Postgres).

CREATE TABLE IF NOT EXISTS principals (
    id BIGSERIAL PRIMARY KEY,
    uuid TEXT NOT NULL UNIQUE,
    type TEXT NOT NULL DEFAULT 'user',
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    time_updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS identities (
    id BIGSERIAL PRIMARY KEY,
    principal_id BIGINT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    sub TEXT NOT NULL,
    latest_login TIMESTAMPTZ,
    UNIQUE (provider, sub)
);

CREATE INDEX IF NOT EXISTS idx_identities_principal_id ON identities(principal_id);

CREATE TABLE IF NOT EXISTS api_keys (
    id BIGSERIAL PRIMARY KEY,
    principal_id BIGINT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    secret_hash TEXT NOT NULL,
    first_eight TEXT NOT NULL,
    note TEXT,
    scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    expiration_time TIMESTAMPTZ,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now(),
    latest_activity TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_api_keys_first_eight ON api_keys(first_eight);
CREATE INDEX IF NOT EXISTS idx_api_keys_principal_id ON api_keys(principal_id);

CREATE TABLE IF NOT EXISTS sessions (
    id BIGSERIAL PRIMARY KEY,
    principal_id BIGINT NOT NULL REFERENCES principals(id) ON DELETE CASCADE,
    uuid TEXT NOT NULL UNIQUE,
    time_last_used TIMESTAMPTZ,
    expiration_time TIMESTAMPTZ NOT NULL,
    revoked BOOLEAN NOT NULL DEFAULT FALSE,
    scopes JSONB NOT NULL DEFAULT '[]'::jsonb,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_sessions_principal_id ON sessions(principal_id);

CREATE TABLE IF NOT EXISTS device_codes (
    id BIGSERIAL PRIMARY KEY,
    device_code TEXT NOT NULL UNIQUE,
    user_code TEXT NOT NULL UNIQUE,
    principal_id BIGINT REFERENCES principals(id) ON DELETE SET NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    interval_seconds INTEGER NOT NULL DEFAULT 5,
    last_polled_at TIMESTAMPTZ,
    time_created TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_device_codes_user_code ON device_codes(user_code);
