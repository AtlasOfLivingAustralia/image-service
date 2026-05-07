package au.org.ala.images

public class CSVColumnDefintion {

    String columnType
    String columnName
    String id = UUID.randomUUID().toString()

    public String toString() {
        return "${columnType}:${columnName}"
    }

    public static CSVColumnDefintion fromString(String str) {
        def bits = str.split("\\:")
        if (bits.size() == 2) {
            def coltype = bits[0] as CSVColumnType // ensure it's a valid column type
            return new CSVColumnDefintion(columnName: bits[1], columnType: coltype.toString())
        }
        return null
    }

}

public enum CSVColumnType {
    property, metadata
}
