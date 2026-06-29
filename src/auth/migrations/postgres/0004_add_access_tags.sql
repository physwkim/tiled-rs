-- Add `access_tags` column to principals (tag-based access control).
ALTER TABLE principals ADD COLUMN IF NOT EXISTS access_tags JSONB NOT NULL DEFAULT '[]'::jsonb;

-- Add `access_tags` column to api_keys (authn_access_tags narrowing).
ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS access_tags JSONB NOT NULL DEFAULT '[]'::jsonb;
