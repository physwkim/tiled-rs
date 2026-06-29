-- Add `role` column to principals so login can derive the correct scope set.
ALTER TABLE principals ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user';
