ALTER TABLE image
    ADD COLUMN IF NOT EXISTS alternate_filename varchar[],
    DROP COLUMN IF EXISTS is_duplicate_of_id;

CREATE INDEX image_alternate_filename_idx ON image (alternate_filename);

