CREATE OR REPLACE FUNCTION export_dataset(uid varchar) RETURNS void AS $$
DECLARE
    output_file CONSTANT varchar := CONCAT(CONCAT( '/data/image-service/exports/images-export-', uid), '.csv');
BEGIN
    EXECUTE format ('
    COPY
        (
        select
            image_identifier as "imageID",
            original_filename as "identifier",
            audience,
            contributor,
            created,
            creator,
            description,
            mime_type as "format",
            license,
            publisher,
            dc_references as "references",
            rights_holder  as "rightsHolder",
            source,
            title,
            type
            from image i
            where data_resource_uid = %L
        )
    TO %L (FORMAT CSV)'
        , uid, output_file);
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION export_dataset_mapping(uid varchar) RETURNS void AS $$
DECLARE
    output_file CONSTANT varchar := CONCAT(CONCAT( '/data/image-service/exports/images-mapping-', uid), '.csv');
BEGIN
    EXECUTE format ('
    COPY
        (
        select
            image_identifier as "imageID",
            original_filename as "url"
            from image i
            where data_resource_uid = %L
        )
    TO %L (FORMAT CSV)'
        , uid, output_file);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION export_mapping() RETURNS void AS $$
DECLARE
    output_file CONSTANT varchar :=  '/data/image-service/exports/images-mapping.csv';
BEGIN
    EXECUTE format ('
    COPY
        (
        select
            data_resource_uid,
            image_identifier as "imageID",
            original_filename as "url"
            from image i
            where data_resource_uid is NOT NULL
        )
    TO %L (FORMAT CSV)'
        , output_file);
END;
$$ LANGUAGE plpgsql;