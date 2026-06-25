-- tags: registry of valid, administrator-defined tag names.
-- A tag must exist here before it can be assigned to a node via
-- TagBasedPolicy::init_node or modify_node.
CREATE TABLE IF NOT EXISTS tags (
    name TEXT PRIMARY KEY
);

-- tag_scopes: per-tag scope assignments.
-- Rows here override the policy's default_scopes for this tag.
-- When no rows exist for a tag the policy falls back to default_scopes,
-- keeping existing deployments working without any configuration.
CREATE TABLE IF NOT EXISTS tag_scopes (
    tag   TEXT NOT NULL REFERENCES tags(name) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    PRIMARY KEY (tag, scope)
);
