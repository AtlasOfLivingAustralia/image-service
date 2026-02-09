-- This generates the exact config block needed to enable config-based storage
-- This query can be run before migration to capture current config
-- or after migration to verify correctness
-- Run via: psql --no-align -h <host> -U <user> -d <db> < R__print-storage-config.sql
SELECT '# =================================================================='::text AS yaml_config
UNION ALL
SELECT '# Generated Storage Location Configuration for application.yml'
UNION ALL
SELECT '# Add this to your application.yml or external config file'
UNION ALL
SELECT '# Generated: ' || CURRENT_TIMESTAMP::text
UNION ALL
SELECT '# =================================================================='
UNION ALL
SELECT ''
UNION ALL
SELECT 'imageservice:'
UNION ALL
SELECT '  storage:'
UNION ALL
SELECT '    locations:'

-- FileSystem Storage Locations
UNION ALL
SELECT CASE WHEN EXISTS (SELECT 1 FROM storage_location WHERE class = 'au.org.ala.images.FileSystemStorageLocation')
                THEN '      # FileSystem Storage Locations'
            ELSE NULL END
    WHERE EXISTS (SELECT 1 FROM storage_location WHERE class = 'au.org.ala.images.FileSystemStorageLocation')
UNION ALL
SELECT '      ' ||
       'fs_' || REPLACE(REPLACE(base_path, '/', '_'), '.', '_') || ':' || E'\n' ||
    '        type: fs' || E'\n' ||
    '        basePath: ''' || base_path || ''''
FROM storage_location
WHERE class = 'au.org.ala.images.FileSystemStorageLocation'

-- S3 Storage Locations
UNION ALL
SELECT CASE WHEN EXISTS (SELECT 1 FROM storage_location WHERE class = 'au.org.ala.images.S3StorageLocation')
                THEN E'\n      # S3 Storage Locations'
            ELSE NULL END
    WHERE EXISTS (SELECT 1 FROM storage_location WHERE class = 'au.org.ala.images.S3StorageLocation')
UNION ALL
SELECT '      ' ||
       's3_' || region || '_' || bucket ||
       CASE WHEN prefix IS NOT NULL AND prefix != ''
         THEN '_' || REPLACE(REPLACE(prefix, '/', '_'), '-', '_')
            ELSE ''
           END || ':' || E'\n' ||
    '        type: s3' || E'\n' ||
    '        region: ' || region || E'\n' ||
    '        bucket: ' || bucket || E'\n' ||
    CASE WHEN prefix IS NOT NULL AND prefix != ''
         THEN '        prefix: ''' || prefix || '''' || E'\n'
         ELSE ''
    END ||
    CASE WHEN access_key IS NOT NULL AND access_key != ''
         THEN '        accessKey: ' || '********  # Replace with actual access key or env var' || E'\n'
         ELSE ''
    END ||
    CASE WHEN secret_key IS NOT NULL AND secret_key != ''
         THEN '        secretKey: ' || '********  # Replace with actual secret or env var' || E'\n'
         ELSE ''
    END ||
    CASE WHEN container_credentials IS NOT NULL
         THEN '        containerCredentials: ' || COALESCE(container_credentials::text, 'false') || E'\n'
         ELSE ''
    END ||
    '        publicRead: ' || COALESCE(public_read::text, 'false') || E'\n' ||
    '        privateAcl: ' || COALESCE(private_acl::text, 'false') || E'\n' ||
    '        redirect: ' || COALESCE(redirect::text, 'false') ||
    CASE WHEN cloudfront_domain IS NOT NULL AND cloudfront_domain != ''
         THEN E'\n' || '        cloudfrontDomain: ' || cloudfront_domain
         ELSE ''
    END
FROM storage_location
WHERE class = 'au.org.ala.images.S3StorageLocation'

-- Swift Storage Locations
UNION ALL
SELECT CASE WHEN EXISTS (SELECT 1 FROM storage_location WHERE class = 'au.org.ala.images.SwiftStorageLocation')
                THEN E'\n      # Swift Storage Locations'
            ELSE NULL END
    WHERE EXISTS (SELECT 1 FROM storage_location WHERE class = 'au.org.ala.images.SwiftStorageLocation')
UNION ALL
SELECT '      ' ||
       'swift_' || REPLACE(container_name, '-', '_') || ':' || E'\n' ||
    '        type: swift' || E'\n' ||
    '        authUrl: ' || auth_url || E'\n' ||
    '        containerName: ' || container_name || E'\n' ||
    '        username: ' || '********  # Replace with actual username or env var' || E'\n' ||
    '        password: ' || '********  # Replace with actual password or env var' || E'\n' ||
    CASE WHEN tenant_id IS NOT NULL AND tenant_id != ''
         THEN '        tenantId: ''' || tenant_id || '''' || E'\n'
         ELSE ''
    END ||
    CASE WHEN tenant_name IS NOT NULL AND tenant_name != ''
         THEN '        tenantName: ''' || tenant_name || '''' || E'\n'
         ELSE ''
    END ||
    '        authenticationMethod: ' || COALESCE(authentication_method::text, 'BASIC') || E'\n' ||
    '        publicContainer: ' || COALESCE(public_container::text, 'false') || E'\n' ||
    '        redirect: ' || COALESCE(redirect::text, 'false')
FROM storage_location
WHERE class = 'au.org.ala.images.SwiftStorageLocation'

-- Footer with instructions
UNION ALL
SELECT ''
UNION ALL
SELECT '# =================================================================='
UNION ALL
SELECT '# Instructions:'
UNION ALL
SELECT '# 1. Copy the above configuration to your application.yml'
UNION ALL
SELECT '# 2. Address any placeholder values (e.g., accessKey, secretKey, username, password)'
UNION ALL
SELECT '# 3. Restart the application to activate config-based storage'
UNION ALL
SELECT '# 4. Monitor logs for: "Registered storage operation" messages'
UNION ALL
SELECT '# 5. Check metrics for: *.nodbhit counters to verify optimization'
UNION ALL
SELECT '# =================================================================='

-- Summary statistics
UNION ALL
SELECT ''
UNION ALL
SELECT '# Summary:'
UNION ALL
SELECT '#   Total storage locations: ' ||
       (SELECT COUNT(*)::text FROM storage_location)
UNION ALL
SELECT '#   FileSystem: ' || (SELECT COUNT(*)::text FROM storage_location WHERE class = 'au.org.ala.images.FileSystemStorageLocation')
UNION ALL
SELECT '#   S3: ' || (SELECT COUNT(*)::text FROM storage_location WHERE class = 'au.org.ala.images.S3StorageLocation')
UNION ALL
SELECT '#   Swift: ' || (SELECT COUNT(*)::text FROM storage_location WHERE class = 'au.org.ala.images.SwiftStorageLocation')
;
-- UNION ALL
-- SELECT '#   Total images: ' || (SELECT COUNT(*)::text FROM image)
-- UNION ALL
-- SELECT '#   Images with storage_location_name: ' ||
--        (SELECT COUNT(*)::text FROM image WHERE storage_location_name IS NOT NULL)
-- ORDER BY yaml_config;
