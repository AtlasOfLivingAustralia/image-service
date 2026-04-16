package au.org.ala.images

import groovy.util.logging.Slf4j
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.avro.io.DatumWriter

import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

@Slf4j
class AvroUtils {

    static final String IDENTIFIER = 'identifier'
    static final String AUDIENCE = 'audience'
    static final String CONTRIBUTOR = "contributor"
    static final String CREATED = 'created'
    static final String CREATOR = "creator"
    static final String DESCRIPTION = 'description'
    static final String LICENSE = 'license'
    static final String PUBLISHER = 'publisher'
    static final String REFERENCES = 'references'
    static final String RIGHTS = 'rights'
    static final String RIGHTS_HOLDER = 'rightsHolder'
    static final String SOURCE = 'source'
    static final String TITLE = 'title'
    static final String TYPE = 'type'

    static final List<String> OPTIONAL_KEYS = [
            AUDIENCE,
            CONTRIBUTOR,
            CREATED,
            CREATOR,
            DESCRIPTION,
            LICENSE,
            PUBLISHER,
            REFERENCES,
            RIGHTS,
            RIGHTS_HOLDER,
            SOURCE,
            TITLE,
            TYPE
    ]


    static File generateTestArchive(boolean setCodec = false) {
        generateTestArchive([['https://www.ala.org.au/app/uploads/2019/05/palm-cockatoo-by-Alan-Pettigrew-1920-1200-CCBY-28072018-640x480.jpg']], setCodec)
    }

    static File generateTestArchive(List<List<String>> urls, boolean setCodec = false) {
        generateTestArchiveWithMetadata(urls.collect { it.collect { url -> ['identifier': url ]}}, false, setCodec)
    }

    static File generateTestArchiveWithMetadata(List<List<Map<String,String>>> records, boolean useSingleRecordIfAble, boolean setCodec = false, boolean skipRightsField = false) {
        try {
            File newArchiveDir = new File("/tmp/image-service-avro-test")
            newArchiveDir.mkdir()
            File newArchive = new File(newArchiveDir, "data.avro.zip");
            FileOutputStream fos = new FileOutputStream(newArchive);
            ZipOutputStream zipOut = new ZipOutputStream(fos);
            List<String> optionalKeys = skipRightsField ? AvroUtils.OPTIONAL_KEYS.findAll {it != RIGHTS } : AvroUtils.OPTIONAL_KEYS

            for (int i = 0; i < records.size(); ++i) {

                Schema multimediaSchema = SchemaBuilder
                        .builder()
                        .record("multimedia")
                        .fields()
                        .requiredString("identifier")
                        .with {assembler ->
                            optionalKeys.each { key ->
                                assembler.optionalString(key)
                            }
                            assembler
                        }
                        .endRecord();

                Schema multimediaRecordSchema = SchemaBuilder
                        .builder()
                        .record("multimediaRecord")
                        .fields()
                        .requiredString("id")
                        .name("multimediaItems").type().nullable().array().items(multimediaSchema)
                        .noDefault().endRecord()

                // generate a test archive
                List<GenericRecord> multimedias = records[i].collect { record ->
                    new GenericRecordBuilder(multimediaSchema)
                            .set("identifier", record.identifier)
                            .with {builder ->
                                optionalKeys.each { key ->
                                    if (record.containsKey(key)) {
                                        builder.set(key, record[key])
                                    }
                                }
                                builder
                            }
                            .build()
                }

                Schema recordSchema
                GenericRecord record
                if (useSingleRecordIfAble && multimedias.size() == 1) {
                    record = multimedias.first()
                    recordSchema = multimediaSchema
                } else {
                    record = new GenericRecordBuilder(multimediaRecordSchema)
                            .set("id", "1")
                            .set("multimediaItems", multimedias)
                            .build();
                    recordSchema = multimediaRecordSchema
                }

                ZipEntry zipEntry = new ZipEntry(records.size() == 1 ? "data.avro" : "data${i}.avro");
                zipOut.putNextEntry(zipEntry);

                DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(recordSchema);
                DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(writer);
                if(setCodec) {
                    log.info(CodecFactory.REGISTERED as String)
                    System.out.println(CodecFactory.REGISTERED)
                    CodecFactory factory = CodecFactory.fromString("null");
                    dataFileWriter.setCodec(factory);
                }
                dataFileWriter.create(recordSchema, zipOut);
                dataFileWriter.append(record);
                dataFileWriter.flush();
            }

            zipOut.close();
            fos.close();

            newArchive
        } catch (e) {
            throw e
        }
    }
}
