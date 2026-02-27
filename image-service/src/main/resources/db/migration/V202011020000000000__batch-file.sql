create table IF NOT EXISTS batch_file_upload (
    id bigint NOT NULL,
    date_completed timestamp without time zone,
    file_path character varying(255) NOT NULL,
    date_created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    status character varying(255) NOT NULL,
    message character varying(255) NOT NULL,
    data_resource_uid character varying(255) NOT NULL,
    md5hash character varying(255) NOT NULL
);

create table IF NOT EXISTS batch_file (
    id bigint NOT NULL,
    record_count bigint NOT NULL,
    date_completed timestamp without time zone,
    file_path character varying(255) NOT NULL,
    processed_count bigint,
    date_created timestamp without time zone NOT NULL,
    last_updated timestamp without time zone NOT NULL,
    status character varying(255) NOT NULL,
    metadata_updates bigint,
    md5hash character varying(255) NOT NULL,
    new_images bigint,
    batch_file_upload_id bigint NOT NULL,
    time_taken_to_load bigint
);

