-- Indicies to allow for efficient deletes on the image table
CREATE INDEX IF NOT EXISTS "outsourced_job_fkey_idx" ON outsourced_job (image_id);
CREATE INDEX IF NOT EXISTS "selected_image_fkey_idx" ON selected_image (image_id);
CREATE INDEX IF NOT EXISTS "subimage_subimage_id_fkey_idx" ON subimage (subimage_id);
CREATE INDEX IF NOT EXISTS "subimage_parent_image_id_fkey_idx" ON subimage (parent_image_id);
CREATE INDEX IF NOT EXISTS "image_parent_id_fkey_idx" ON image (parent_id);
CREATE INDEX IF NOT EXISTS "image_date_deleted_idx" ON image (date_deleted);