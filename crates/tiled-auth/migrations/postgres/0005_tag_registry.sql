-- tags: registry of valid, administrator-defined tag names.
CREATE TABLE IF NOT EXISTS tags (
    name TEXT PRIMARY KEY
);

-- tag_scopes: per-tag scope assignments.
-- Empty rows for a tag → fall back to policy's default_scopes.
CREATE TABLE IF NOT EXISTS tag_scopes (
    tag   TEXT NOT NULL REFERENCES tags(name) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    PRIMARY KEY (tag, scope)
);
