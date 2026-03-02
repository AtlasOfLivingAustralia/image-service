-- Add JSONB column for metadata to image table
-- This is part of a zero-downtime migration from the EAV-style image_meta_data_item table

-- Add the new column (nullable for now to allow gradual migration)
ALTER TABLE image ADD COLUMN IF NOT EXISTS metadata jsonb;

-- Add a GIN index for efficient JSONB queries
CREATE INDEX IF NOT EXISTS idx_image_metadata ON image USING gin(metadata);

-- Add an index to track which images have been migrated (have metadata)
CREATE INDEX IF NOT EXISTS idx_image_metadata_not_null ON image(id) WHERE metadata IS NOT NULL;

-- Note: We do NOT drop the image_meta_data_item table yet as we need to maintain backwards compatibility
-- during the migration period

-- When the drop table migration is added, we need to add an update to move any missing metadata to the new column
