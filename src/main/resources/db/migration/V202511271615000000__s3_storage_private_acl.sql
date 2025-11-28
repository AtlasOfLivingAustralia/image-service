-- Add explicit private_acl flag for S3 storage locations
-- Allows UI to request S3 Canned Private ACL explicitly; when false and publicRead is false,
-- we avoid sending any ACL header so bucket defaults apply.

ALTER TABLE storage_location
  ADD COLUMN IF NOT EXISTS private_acl BOOLEAN;

-- Default existing rows to false for S3 storage locations where null
UPDATE storage_location
  SET private_acl = NOT public_read
  WHERE class = 'au.org.ala.images.S3StorageLocation' AND private_acl IS NULL;
