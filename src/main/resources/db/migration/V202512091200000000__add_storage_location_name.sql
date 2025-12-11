-- Step 1: Add nullable storage_location_name column
ALTER TABLE IF EXISTS image
ADD COLUMN IF NOT EXISTS storage_location_name TEXT;

COMMENT ON COLUMN image.storage_location_name IS 'Name of the storage location from config (enables DB-free lookups)';

-- Step 2: Generate names from existing storage locations
-- For FileSystemStorageLocation: use sanitized basePath
UPDATE image i
SET storage_location_name = 'fs_' || REPLACE(REPLACE(sl.base_path, '/', '_'), '.', '_')
FROM storage_location sl
WHERE i.storage_location_id = sl.id
  AND sl.class = 'au.org.ala.images.FileSystemStorageLocation'
  AND i.storage_location_name IS NULL;

-- For S3StorageLocation: use region_bucket_prefix
UPDATE image i
SET storage_location_name = 's3_' || sl.region || '_' || sl.bucket ||
    CASE WHEN sl.prefix IS NOT NULL AND sl.prefix != ''
         THEN '_' || REPLACE(REPLACE(sl.prefix, '/', '_'), '-', '_')
         ELSE ''
    END
FROM storage_location sl
WHERE i.storage_location_id = sl.id
  AND sl.class = 'au.org.ala.images.S3StorageLocation'
  AND i.storage_location_name IS NULL;

-- For SwiftStorageLocation: use container name
UPDATE image i
SET storage_location_name = 'swift_' || REPLACE(sl.container_name, '-', '_')
FROM storage_location sl
WHERE i.storage_location_id = sl.id
  AND sl.class = 'au.org.ala.images.SwiftStorageLocation'
  AND i.storage_location_name IS NULL;

-- Step 3: Add index for performance
CREATE INDEX IF NOT EXISTS idx_image_storage_location_name ON image(storage_location_name);

-- Step 4: Add combined index for common query pattern
CREATE INDEX IF NOT EXISTS idx_image_identifier_storage_name ON image(image_identifier, storage_location_name);

-- Step 5: Backfill complete - make storage_location_id nullable and adjust FK to set NULL on delete
ALTER TABLE IF EXISTS image
    ALTER COLUMN storage_location_id DROP NOT NULL;

ALTER TABLE IF EXISTS image
    DROP CONSTRAINT IF EXISTS image_storage_location_fk;

ALTER TABLE image
    ADD CONSTRAINT image_storage_location_fk
        FOREIGN KEY (storage_location_id) REFERENCES storage_location(id) ON DELETE SET NULL;

ALTER TABLE image
    ADD CONSTRAINT image_storage_location_or_name_not_null
    CHECK (num_nulls(storage_location_id, storage_location_name) <= 1);

-- Step 6: Validation - report any images without storage_location_name
DO $$
DECLARE
    missing_count INTEGER;
    total_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO missing_count FROM image WHERE storage_location_name IS NULL;
    SELECT COUNT(*) INTO total_count FROM image;

    IF missing_count > 0 THEN
        RAISE WARNING 'WARNING: % out of % images do not have storage_location_name set', missing_count, total_count;
    ELSE
        RAISE NOTICE 'SUCCESS: All % images have storage_location_name set', total_count;
    END IF;
END $$;


