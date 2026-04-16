alter table storage_location
    add redirect boolean;

update storage_location set redirect = false
where class in ('au.org.ala.images.S3StorageLocation', 'au.org.ala.images.SwiftStorageLocation');