create table storage_location
(
    id bigserial primary key,
    version bigint not null,
    date_created timestamp not null default current_timestamp,
    last_updated timestamp not null default current_timestamp,
    class text not null,
    public_read boolean,
    region text,
    prefix text,
    secret_key text,
    bucket text,
    access_key text,
    hostname text,
    path_style_access boolean,
    base_path text
);

create index storage_location_class on storage_location (class);

alter table image
    add storage_location_id bigint;

insert into storage_location (version, class, base_path, date_created, last_updated)
values (0, 'au.org.ala.images.FileSystemStorageLocation', '${imageRoot}', default, default);

DROP INDEX IF EXISTS image_datetaken_idx;
DROP INDEX IF EXISTS image_dateuploaded;
DROP INDEX IF EXISTS image_md5hash_idx;
DROP INDEX IF EXISTS image_originalfilename_idx;
DROP INDEX IF EXISTS imageidentifier_idx;
DROP INDEX IF EXISTS new_image_originalfilename_idx;

update image set storage_location_id = (select id from storage_location limit 1);

CREATE INDEX image_datetaken_idx ON image USING btree (date_taken DESC);
CREATE INDEX image_dateuploaded ON image USING btree (date_uploaded, id);
CREATE INDEX image_md5hash_idx ON image USING btree (id, contentmd5hash);
CREATE INDEX image_originalfilename_idx ON image USING btree (original_filename_old);
CREATE INDEX imageidentifier_idx ON image USING btree (image_identifier);
CREATE INDEX new_image_originalfilename_idx ON image USING btree (original_filename);

alter table image
    alter column storage_location_id set not null,
    add constraint image_storage_location_fk
        foreign key (storage_location_id) references storage_location(id) on delete cascade;
