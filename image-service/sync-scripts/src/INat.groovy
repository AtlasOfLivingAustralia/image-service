@Grab(group = 'net.sf.opencsv', module = 'opencsv', version = '2.3')
import au.com.bytecode.opencsv.CSVReader
import au.com.bytecode.opencsv.CSVParser
import au.com.bytecode.opencsv.CSVWriter

reader =  new CSVReader(new FileReader(new File("/tmp/inaturalist-dwca/media.csv")))

newPrefix = "https://inaturalist-open-data.s3.amazonaws.com/photos/"
line = reader.readNext() //headers

movedUrls = []
while (line) {
  line = reader.readNext()
  if (line) {
    url = line[3]
    if (url.startsWith(newPrefix)) {
      movedUrls << url.substring(newPrefix.length())
    }
  }
}
reader.close()
println("New URLs = " + movedUrls.size())

oldPrefix = "https://static.inaturalist.org/photos/"

reader2 =  new CSVReader(new FileReader(new File("/tmp/image-mapping-dr1411.csv")))
line = reader2.readNext()
oldUrls = [:]
a = 0
while (line) {
  a = a + 1
  line = reader2.readNext()
  if (line) {
    imageID = line[0]
    url = line[1]
    if (url.startsWith(oldPrefix)) {
      oldUrl = url.substring(oldPrefix.length())
      oldUrls.put(oldUrl,imageID )
    }
  }
}
reader2.close()
println("Old URLs = " + oldUrls.size())

writer = new FileWriter(new File("/tmp/updateInat.sql"))

movedUrls.each { movedUrl ->

  imageIdentifier = oldUrls.get(movedUrl)
  if (imageIdentifier) {
    originalFilename = "https://inaturalist-open-data.s3.amazonaws.com/photos/" + movedUrl
    writer.write("UPDATE image set original_filename = '${originalFilename}' where image_identifier = '${imageIdentifier}';\n")
  }
}
writer.flush()
writer.close()