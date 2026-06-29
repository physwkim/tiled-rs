-- Add `access_tags` column to principals: the set of tag strings a principal
-- is allowed to access (tag-based access control, mirrors Python
-- TagBasedAccessPolicy / get_tags_from_scope). Stored as a JSON array.
ALTER TABLE principals ADD COLUMN access_tags TEXT NOT NULL DEFAULT '[]';

-- Add `access_tags` column to api_keys: when present, restricts the effective
-- tag set to the intersection of the key's access_tags and the principal's
-- access_tags (authn_access_tags narrowing, Python access_policies.py:409-411).
ALTER TABLE api_keys ADD COLUMN access_tags TEXT NOT NULL DEFAULT '[]';
