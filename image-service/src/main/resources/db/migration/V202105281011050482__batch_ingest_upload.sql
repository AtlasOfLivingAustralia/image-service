CREATE INDEX IF NOT EXISTS "image_dataresourceuid_idx" ON image (data_resource_uid);

DROP INDEX IF EXISTS "image_alternate_filename_idx";
CREATE INDEX "image_alternate_filename_idx" ON image USING GIN (alternate_filename);

ALTER TABLE "image"
    DROP CONSTRAINT IF EXISTS "fk97icgupea1fsbuj4vvc1i8sut",
    ADD CONSTRAINT "image_recognised_license_fk" FOREIGN KEY (recognised_license_id) REFERENCES license(id);