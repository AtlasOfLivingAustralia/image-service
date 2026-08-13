ALTER TABLE storage_location
  ADD COLUMN IF NOT EXISTS cloudfront_domain VARCHAR(255) NULL;

UPDATE storage_location
  SET cloudfront_domain = ''
  WHERE class = 'au.org.ala.images.S3StorageLocation';
