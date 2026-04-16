-- Add a hash index for faster lookups on the contentmd5hash column when the md5 hash isn't in the image table.
CREATE INDEX IF NOT EXISTS "image_md5hash_hash_idx" ON image USING hash (contentmd5hash);
