package au.org.ala.images

enum MetaDataSourceType {
    Embedded, UserDefined, SystemDefined
    public String toString() {
        return name();
    }
}
