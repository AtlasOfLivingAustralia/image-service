package au.org.ala.images

import com.amazonaws.services.rekognition.model.DetectModerationLabelsRequest
import com.amazonaws.services.rekognition.model.DetectModerationLabelsResult
import com.amazonaws.services.rekognition.model.ModerationLabel
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.rekognition.AmazonRekognitionClient
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.rekognition.model.Image
import org.springframework.web.multipart.MultipartFile
import javax.imageio.ImageIO
import com.amazonaws.services.rekognition.model.S3Object;

import java.awt.image.BufferedImage

class ImageRecognitionService {

    AmazonRekognitionClient rekognitionClient
    AmazonS3Client s3Client

    private addImageToS3FromUrl(String filePath) {

        URL url = new URL(filePath);
        BufferedImage img = ImageIO.read(url)
        File file = new File("/tmp/temp-image.jpg")
        ImageIO.write(img, "jpg", file)
        s3Client.putObject("temp-upload-images","temp-image", file)
    }

    private addImageToS3FromFile(MultipartFile file) {
        s3Client.putObject("temp-upload-images","temp-image", new ByteArrayInputStream(file?.bytes),
                generateMetadata(file.contentType, null, file.size))
    }

    private deleteImageS3(String filename) {
        s3Client.deleteObject("temp-upload-images", filename)
    }

    private deleteImageFile(String filename) {
        File file = new File("/tmp/$filename")
        file.delete()
    }

    def checkImageContent(MultipartFile file, String filePath) {

        if(filePath){
            addImageToS3FromUrl(filePath)
        }
        else{
            addImageToS3FromFile(file)
        }

        List labels = detectModLabels()
        if(filePath){
            deleteImageFile("temp-image.jpg")
        }
        deleteImageS3("temp-image")

        return labels
    }

    private detectModLabels() {
        try {
            List labels = []
            DetectModerationLabelsRequest request = new DetectModerationLabelsRequest()
                    .withImage(new Image().withS3Object(new S3Object().withBucket("temp-upload-images").withName("temp-image")))

            DetectModerationLabelsResult result = rekognitionClient.detectModerationLabels(request);

            for (ModerationLabel label : result.moderationLabels) {
                labels.add(label.getName())
                System.out.println(label.getName() + " : " + label.getConfidence());
            }
            return labels

        } catch (Exception e) {
            e.printStackTrace()
        }
    }

    private ObjectMetadata generateMetadata(String contentType, String contentDisposition = null, Long length = null) {
        ObjectMetadata metadata = new ObjectMetadata()
        metadata.setContentType(contentType)
        if (contentDisposition) {
            metadata.setContentDisposition(contentDisposition)
        }
        if (length != null) {
            metadata.setContentLength(length)
        }
        def acl = CannedAccessControlList.Private
        metadata.setHeader('x-amz-acl', acl.toString())
        metadata.cacheControl = ('private') + ',max-age=31536000'
        return metadata
    }
}
