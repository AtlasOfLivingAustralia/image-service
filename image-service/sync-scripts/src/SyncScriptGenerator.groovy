import au.com.bytecode.opencsv.CSVReader

reader =  new CSVReader(new FileReader(new File("/Users/mar759/missing-images.csv")))
writer = new FileWriter(new File("/Users/mar759/sync-script.ssh"))

line = reader.readNext() //headers
line = reader.readNext()

a = 0

while (line){
  a += 1
  writer.write("ssh aws-image-service.ala.org.au 'mkdir -p ${line[1]}'")
  writer.write("\n")
  writer.write("rsync -r -e ssh ${line[1].replaceAll("image-service", "images")} aws-image-service.ala.org.au:${line[1].substring(0, line[1].lastIndexOf("/"))}")
  writer.write("\n")
  writer.flush()
  line = reader.readNext()
  if(a % 100){
    writer.write("echo 'Sync-ed ${a}'")
    writer.write("\n")
  }
}
writer.write("echo 'Finished ${a}'")
writer.write("\n")

writer.flush()
writer.close()
reader.close()
