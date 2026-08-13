ALTER TABLE failed_upload
    ADD PRIMARY KEY (url),
    ALTER COLUMN date_created TYPE timestamp without time zone;

UPDATE failed_upload
SET date_created = date_created AT TIME ZONE
    (SELECT current_setting('TIMEZONE'));

ALTER TABLE image
     ADD CONSTRAINT image_is_duplicate_of_id_fk FOREIGN KEY (is_duplicate_of_id) REFERENCES image (id)
         ON DELETE CASCADE;
