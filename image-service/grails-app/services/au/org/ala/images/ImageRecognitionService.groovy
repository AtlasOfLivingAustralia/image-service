package au.org.ala.images

import groovy.util.logging.Slf4j
import org.springframework.web.multipart.MultipartFile
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.rekognition.RekognitionClient
import software.amazon.awssdk.services.rekognition.model.DetectFacesRequest
import software.amazon.awssdk.services.rekognition.model.DetectFacesResponse
import software.amazon.awssdk.services.rekognition.model.DetectModerationLabelsRequest
import software.amazon.awssdk.services.rekognition.model.DetectModerationLabelsResponse
import software.amazon.awssdk.services.rekognition.model.FaceDetail
import software.amazon.awssdk.services.rekognition.model.Image
import software.amazon.awssdk.services.rekognition.model.ModerationLabel
import software.amazon.awssdk.services.rekognition.model.S3Object
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.services.sagemakerruntime.SageMakerRuntimeClient
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointRequest
import software.amazon.awssdk.services.sagemakerruntime.model.InvokeEndpointResponse

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.awt.image.BufferedImageOp
import java.awt.image.ColorModel
import java.awt.image.ConvolveOp
import java.awt.image.Kernel
import java.util.UUID

@Slf4j
class ImageRecognitionService {

    RekognitionClient rekognitionClient
    S3Client s3Client
    def grailsApplication
    SageMakerRuntimeClient sageMakerRuntime

    private addImageToS3FromUrl(String filePath, String bucket, String tempFileName) {

        URL url = new URL(filePath)
        BufferedImage img = ImageIO.read(url)
        File file = new File("/tmp/${tempFileName}.jpg")
        ImageIO.write(img, "jpg", file)
        s3Client.putObject(putObjectRequest(bucket, tempFileName, 'image/jpeg'), RequestBody.fromFile(file))
    }

    private addImageToS3FromFile(MultipartFile file, String bucket, String tempFileName) {
        byte[] bytes = file?.bytes
        s3Client.putObject(putObjectRequest(bucket, tempFileName, file?.contentType), RequestBody.fromBytes(bytes))
    }

    def addImageToS3FromBytes(byte[] bytes, String bucket, String tempFileName, String contentType) {
        s3Client.putObject(putObjectRequest(bucket, tempFileName, contentType), RequestBody.fromBytes(bytes))
    }

    /**
     * Detect and blur faces using a temporary S3 object required by Rekognition.
     * Rekognition supports JPEG and PNG input; other content types are returned unchanged.
     */
    byte[] blurHumanFaces(byte[] bytes, String contentType) {
        if (!bytes || !(contentType?.toLowerCase() in ['image/jpeg', 'image/jpg', 'image/png'])) {
            return bytes
        }

        String bucket = grailsApplication.config.getProperty(
                'aws.tempImageBucket', String, 'ala-image-service-test-uploads-production')
        String keyPrefix = grailsApplication.config.getProperty('aws.tempImageName', String, 'temp-image')
        String key = "${keyPrefix}-${UUID.randomUUID()}"

        try {
            addImageToS3FromBytes(bytes, bucket, key, contentType)
            List<FaceDetail> faces = detectFaces(bucket, key)
            return faces ? blurFaces(bucket, key, faces, contentType) : bytes
        } finally {
            try {
                deleteImageS3(bucket, key)
            } catch (Exception cleanupError) {
                log.warn('Unable to delete temporary face-detection object s3://{}/{}', bucket, key, cleanupError)
            }
        }
    }

    private deleteImageS3(String bucket, String filename) {
        s3Client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(filename).build())
    }

    private deleteImageFile(String filename) {
        File file = new File("/tmp/$filename")
        file.delete()
    }

    def cleanup(filePath) {
        boolean faceBlurringEnabled = grailsApplication.config.getProperty(
                'images.faceBlurring.enabled', Boolean, false)
        if (!faceBlurringEnabled) {
            return
        }

        String tempImageBucket = grailsApplication.config.getProperty('aws.tempImageBucket', String, "ala-image-service-test-uploads-production")
        String tempImageName = grailsApplication.config.getProperty('aws.tempImageName', String, "temp-image")
        if (filePath) {
            deleteImageFile("${tempImageName}.jpg")
        }
        deleteImageS3(tempImageBucket, tempImageName)
    }

    def checkImageContent(MultipartFile file, String filePath) {

        String tempImageBucket = grailsApplication.config.getProperty('aws.tempImageBucket', String, "ala-image-service-test-uploads-production")
        String tempImageName = grailsApplication.config.getProperty('aws.tempImageName', String, "temp-image")

        try {
            if (filePath) {
                addImageToS3FromUrl(filePath, tempImageBucket, tempImageName)
            } else {
                addImageToS3FromFile(file, tempImageBucket, tempImageName)
            }

            List labels = detectModLabels(tempImageBucket, tempImageName)
            if (labels) {
                return [success: false, message: "Detected inappropriate content: $labels"]
            }

            def roadkillEndpointEnabled = grailsApplication.config.getProperty('aws.sagemaker.enabled', boolean, false)

            if(roadkillEndpointEnabled) {
                boolean ifRoadKill = detectRoadkill(tempImageBucket, tempImageName)
                if (ifRoadKill) {
                    return [success: false, message: "Detected road kill"]
                }
            }
            return [success: true]
        }
        catch (Exception e) {
            throw e
        }
    }

    private detectModLabels(String bucket, String tempFileName) {
        try {
            List labels = []
            def acceptingLabel = "Blood & Gore"
            DetectModerationLabelsRequest request = DetectModerationLabelsRequest.builder()
                    .image(rekognitionImage(bucket, tempFileName))
                    .build()

            DetectModerationLabelsResponse result = rekognitionClient.detectModerationLabels(request)

            for (ModerationLabel label : result.moderationLabels()) {
                labels.add(label.name())
            }
            if(labels.contains(acceptingLabel)) {
                labels = []
            }
            return labels

        } catch (Exception e) {
            e.printStackTrace()
        }
    }

    def detectFaces(String bucket, String tempFileName) {

        DetectFacesRequest faceDetectRequest = DetectFacesRequest.builder()
                .image(rekognitionImage(bucket, tempFileName))
                .build()

        DetectFacesResponse faceDetectResult = rekognitionClient.detectFaces(faceDetectRequest)

        return faceDetectResult.faceDetails()
    }

    private detectRoadkill(String bucket, String tempFileName) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucket).key(tempFileName).build()
        byte[] byteArray = s3Client.getObject(getObjectRequest).withCloseable { it.readAllBytes() }

        InvokeEndpointRequest invokeEndpointRequest = InvokeEndpointRequest.builder()
                .contentType('application/octet-stream')
                .body(SdkBytes.fromByteArray(byteArray))
                .endpointName(grailsApplication.config.getProperty('aws.sagemaker.endpointName', String, ""))
                .accept('application/json')
                .build()

        InvokeEndpointResponse invokeEndpointResult = sageMakerRuntime.invokeEndpoint(invokeEndpointRequest)
        String response = invokeEndpointResult.body().asUtf8String()
        return response == "Roadkill"
    }

    private static PutObjectRequest putObjectRequest(String bucket, String key, String contentType,
                                                     String contentDisposition = null) {
        PutObjectRequest.Builder request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .acl(ObjectCannedACL.PRIVATE)
                .cacheControl('private,max-age=31536000')
        if (contentDisposition) {
            request.contentDisposition(contentDisposition)
        }
        if (contentType) {
            request.contentType(contentType)
        }
        return request.build()
    }

    private static Image rekognitionImage(String bucket, String key) {
        S3Object object = S3Object.builder().bucket(bucket).name(key).build()
        return Image.builder().s3Object(object).build()
    }

    def blurFaces(String bucket, String tempFileName, List<FaceDetail> faceDetails,
                  String contentType = 'image/jpeg') {

        GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(tempFileName).build()
        byte[] imageBytes = s3Client.getObject(request).withCloseable { it.readAllBytes() }
        BufferedImage image = ImageIO.read(new ByteArrayInputStream(imageBytes))
        if (image == null) {
            throw new IOException("Unable to decode image ${tempFileName}")
        }

        int radius = 20
        int size = radius + (image.width/50) as int
        float weight = 1.0f / (size * size);
        float[] matrix = new float[size * size];

        for (int i = 0; i < matrix.length; i++) {
            matrix[i] = weight;
        }

        BufferedImageOp op = new ConvolveOp(new Kernel(size, size, matrix), ConvolveOp.EDGE_NO_OP, null)

        faceDetails.each { face ->
            def fullWidth = image.width
            def fullHeight = image.height
            def margin = 10
            int left = Math.max(0, ((face.boundingBox().left() * fullWidth) as int) - margin)
            int top = Math.max(0, ((face.boundingBox().top() * fullHeight) as int) - margin)
            int right = Math.min(fullWidth,
                    (((face.boundingBox().left() + face.boundingBox().width()) * fullWidth) as int) + margin)
            int bottom = Math.min(fullHeight,
                    (((face.boundingBox().top() + face.boundingBox().height()) * fullHeight) as int) + margin)
            if (right <= left || bottom <= top) {
                log.warn('Skipping invalid face bounding box {} for image {}', face.boundingBox(), tempFileName)
                return
            }
            def dest = image.getSubimage(
                    left, top, right - left, bottom - top)
            ColorModel cm = dest.getColorModel()
            def src = new BufferedImage(cm, dest.copyData(dest.getRaster().createCompatibleWritableRaster()), cm.isAlphaPremultiplied(),
                    null).getSubimage(0,0,dest.getWidth(), dest.getHeight())
            op.filter(src, dest)
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        String outputFormat = contentType?.equalsIgnoreCase('image/png') ? 'png' : 'jpg'
        ImageIO.write(image, outputFormat, baos)
        byte[] bytes = baos.toByteArray()
        baos.close()
        return bytes
    }
}
