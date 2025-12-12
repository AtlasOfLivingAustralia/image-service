-- Add error tracking fields to failed_upload table
ALTER TABLE failed_upload
    ADD COLUMN IF NOT EXISTS error_message text,
    ADD COLUMN IF NOT EXISTS http_status_code integer;

-- Add comments for documentation
COMMENT ON COLUMN failed_upload.error_message IS 'Detailed error message from the failed upload attempt';
COMMENT ON COLUMN failed_upload.http_status_code IS 'HTTP status code if the failure was due to an HTTP error';

