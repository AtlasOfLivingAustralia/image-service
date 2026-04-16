## Intro

You can generate a non-signed debian package via:

```bash
debuild -us -uc -b
```
in the parent of this directory. This will generate the deb file in the parent directory of this repository.

You can increase changelog version and comments with `dch` utility, like with `dch -i` that increase minor version.

Add to your .gitignore:
```
*/*.debhelper
*/*.debhelper.log
*.buildinfo
*.substvars
debian/files
debian/ala-image
```

## Looking for inspiration?

You can see [tomcat7-examples package source](https://salsa.debian.org/java-team/tomcat7/tree/master/debian) for inspiration of tomcat7 packages and also about how to create multiple debian packages from a same source repository.

Also `dbconfig-common` package have some samples in `/usr/share/doc/dbconfig-common/examples/` for mysql and postgresql debian configuration tasks for packages.

## Testing

You can test the generated package without install it with `piuparts` like:

```bash
sudo piuparts -D ubuntu -d xenial -d bionic ../ala-collectory_1.6.2-1_all.deb
```
in this way you can also test it in different releases.

Read `/usr/share/doc/piuparts/README.*` for more usage samples.

## No interactive install

You can preseed this package with something similar to (if this package uses mysql):

```bash
echo "ala-image ala-image/mysql/admin-pass password $DB_ROOT_PWD" | debconf-set-selections && \
echo "ala-image ala-image/dbconfig-install boolean true" | debconf-set-selections && \
echo "ala-image ala-image/dbconfig-upgrade boolean true" | debconf-set-selections

cat > /etc/dbconfig-common/ala-image.conf <<EOF
dbc_dbname='image'
dbc_dbuser='image'
dbc_dbpass='password'
EOF

export DEBCONF_FRONTEND=noninteractive
apt-get install -yq --force-yes ala-image
```

Also you can install `dbconfig-no-thanks` to avoid db questions.

## TODO

- [ ] pg_instance, extensions: ["citext", "pgcrypto"], # from ala-images.yml playbook
- [ ] postgis_template 
- [ ] data/image-service/setup/image-service-export.sql is dependent of the images domain

## CODE DUPLICATED 

Code duplicated from `ala-install` of similar:

- data/image-service/setup/image-service-export.sql
- data/image-service/config/image-service-config.template
- data/image-service/config/logback.groovy
