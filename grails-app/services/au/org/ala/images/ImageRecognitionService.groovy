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
    def grailsApplication

    private addImageToS3FromUrl(String filePath, String bucket, String tempFileName) {

        URL url = new URL(filePath);
        BufferedImage img = ImageIO.read(url)
        File file = new File("/tmp/${tempFileName}.jpg")
        ImageIO.write(img, "jpg", file)
        s3Client.putObject(bucket, tempFileName, file)
    }

    private addImageToS3FromFile(MultipartFile file, String bucket, String tempFileName) {
        s3Client.putObject(bucket, tempFileName, new ByteArrayInputStream(file?.bytes),
                generateMetadata(file.contentType, null, file.size))
    }

    private deleteImageS3(String bucket, String filename) {
        s3Client.deleteObject(bucket, filename)
    }

    private deleteImageFile(String filename) {
        File file = new File("/tmp/$filename")
        file.delete()
    }

    def checkImageContent(MultipartFile file, String filePath) {

        String tempImageBucket = grailsApplication.config.getProperty('aws.tempImageBucket', String, "temp-upload-images")
        String tempImageName = grailsApplication.config.getProperty('aws.tempImageName', String, "temp-image")

        try {
            if (filePath) {
                addImageToS3FromUrl(filePath, tempImageBucket, tempImageName)
            } else {
                addImageToS3FromFile(file, tempImageBucket, tempImageName)
            }

            List labels = detectModLabels(tempImageBucket, tempImageName)
            return labels
        }
        catch (Exception e) {
            throw e
        }
        finally {
            if (filePath) {
                deleteImageFile("${tempImageName}.jpg")
            }
            deleteImageS3(tempImageBucket, tempImageName)
        }
    }

    private detectModLabels(String bucket, String tempFileName) {
        try {
            List labels = []
            DetectModerationLabelsRequest request = new DetectModerationLabelsRequest()
                    .withImage(new Image().withS3Object(new S3Object().withBucket(bucket).withName(tempFileName)))

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
