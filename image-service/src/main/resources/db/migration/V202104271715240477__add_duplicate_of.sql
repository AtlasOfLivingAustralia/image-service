alter table image
    add is_duplicate_of_id bigint;

alter table batch_file
    add error_count bigint;

create table IF NOT EXISTS failed_upload
(
    url text,
    date_created timestamp not null default current_timestamp
);