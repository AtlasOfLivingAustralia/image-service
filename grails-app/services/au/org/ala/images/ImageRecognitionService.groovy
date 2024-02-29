package au.org.ala.images

import com.amazonaws.services.rekognition.model.DetectFacesRequest
import com.amazonaws.services.rekognition.model.DetectFacesResult
import com.amazonaws.services.rekognition.model.DetectModerationLabelsRequest
import com.amazonaws.services.rekognition.model.DetectModerationLabelsResult
import com.amazonaws.services.rekognition.model.FaceDetail
import com.amazonaws.services.rekognition.model.ModerationLabel
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.rekognition.AmazonRekognitionClient
import com.amazonaws.services.s3.model.CannedAccessControlList
import com.amazonaws.services.s3.model.GetObjectRequest
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.rekognition.model.Image
import com.amazonaws.services.sagemakerruntime.AmazonSageMakerRuntime
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointRequest
import com.amazonaws.services.sagemakerruntime.model.InvokeEndpointResult
import org.springframework.web.multipart.MultipartFile

import javax.imageio.ImageIO
import com.amazonaws.services.rekognition.model.S3Object
import org.apache.commons.io.IOUtils

import javax.imageio.stream.ImageInputStream
import java.awt.image.BufferedImage
import java.awt.image.BufferedImageOp
import java.awt.image.ColorModel
import java.awt.image.ConvolveOp
import java.awt.image.Kernel
import java.nio.ByteBuffer

class ImageRecognitionService {

    AmazonRekognitionClient rekognitionClient
    AmazonS3Client s3Client
    def grailsApplication
    AmazonSageMakerRuntime sageMakerRuntime

    private addImageToS3FromUrl(String filePath, String bucket, String tempFileName) {

        URL url = new URL(filePath)
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

    def cleanup(filePath) {
        String tempImageBucket = grailsApplication.config.getProperty('aws.tempImageBucket', String, "temp-upload-images")
        String tempImageName = grailsApplication.config.getProperty('aws.tempImageName', String, "temp-image")
        if (filePath) {
            deleteImageFile("${tempImageName}.jpg")
        }
        deleteImageS3(tempImageBucket, tempImageName)
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
            DetectModerationLabelsRequest request = new DetectModerationLabelsRequest()
                    .withImage(new Image().withS3Object(new S3Object().withBucket(bucket).withName(tempFileName)))

            DetectModerationLabelsResult result = rekognitionClient.detectModerationLabels(request)

            for (ModerationLabel label : result.moderationLabels) {
                labels.add(label.getName())
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

        DetectFacesRequest faceDetectRequest = new DetectFacesRequest()
                .withImage(new Image().withS3Object(new S3Object().withBucket(bucket).withName(tempFileName)))

        DetectFacesResult faceDetectResult = rekognitionClient.detectFaces(faceDetectRequest)

        return faceDetectResult.faceDetails
    }

    private detectRoadkill(String bucket, String tempFileName) {

        def object = s3Client.getObject(new GetObjectRequest(bucket, tempFileName))
        InputStream objectData = object.getObjectContent()
        byte[] byteArray = IOUtils.toByteArray(objectData)

        InvokeEndpointRequest invokeEndpointRequest = new InvokeEndpointRequest()
        invokeEndpointRequest.setContentType("application/octet-stream")
        ByteBuffer buf = ByteBuffer.wrap(byteArray)
        invokeEndpointRequest.setBody(buf)
        invokeEndpointRequest.setEndpointName(grailsApplication.config.getProperty('aws.sagemaker.endpointName', String, ""))
        invokeEndpointRequest.setAccept("application/json")

        InvokeEndpointResult invokeEndpointResult = sageMakerRuntime.invokeEndpoint(invokeEndpointRequest)
        objectData.close()
        String response = new String(invokeEndpointResult.getBody().array())
        return response == "Roadkill"
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

    def blurFaces(String bucket, String tempFileName, List<FaceDetail> faceDetails) {

        def o = s3Client.getObject(bucket, tempFileName)
        ImageInputStream iin = ImageIO.createImageInputStream(o.getObjectContent())
        BufferedImage image = ImageIO.read(iin)

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
            def dest = image.getSubimage(
                    ((face.boundingBox.left * fullWidth) as int) - margin,
                    ((face.boundingBox.top * fullHeight) as int) - margin,
                    ((face.boundingBox.width * fullWidth) as int) + margin * 2,
                    ((face.boundingBox.height * fullHeight) as int) + margin * 2)
            ColorModel cm = dest.getColorModel()
            def src = new BufferedImage(cm, dest.copyData(dest.getRaster().createCompatibleWritableRaster()), cm.isAlphaPremultiplied(),
                    null).getSubimage(0,0,dest.getWidth(), dest.getHeight())
            op.filter(src, dest)
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream()
        ImageIO.write(image, "jpg", baos)
        byte[] bytes = baos.toByteArray()
        baos.close()
        return bytes
    }
}
