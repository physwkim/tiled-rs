-- Add `role` column to principals so login can derive the correct scope set.
-- Default 'user'. Operators upgrade specific principals to 'admin' via direct
-- SQL or a future admin endpoint.
ALTER TABLE principals ADD COLUMN role TEXT NOT NULL DEFAULT 'user';
