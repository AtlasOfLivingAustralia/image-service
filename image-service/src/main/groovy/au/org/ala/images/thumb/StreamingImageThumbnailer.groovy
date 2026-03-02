package au.org.ala.images.thumb

import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.thumb.ThumbnailingResult
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j

import java.awt.Color

/**
 * Streaming thumbnailer that uses external tools (vips, imagemagick) to generate thumbnails
 * without loading the full image into memory. Streams bytes directly from stdin to stdout.
 */
@Slf4j
@CompileStatic
class StreamingImageThumbnailer implements IImageThumbnailer {

    private final CommandExecutor commandExecutor
    private final String tool // 'vips' or 'magick'
    private final long timeoutSeconds

    StreamingImageThumbnailer(CommandExecutor commandExecutor, String tool = 'vips', long timeoutSeconds = 30) {
        this.commandExecutor = commandExecutor
        this.tool = tool
        this.timeoutSeconds = timeoutSeconds
    }

    @Override
    List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        List<ThumbnailingResult> results = new ArrayList<>()

        for (ThumbDefinition thumbDef : thumbDefs) {
            try {
                ThumbnailingResult result = generateSingleThumbnail(imageBytes, byteSinkFactory, thumbDef)
                if (result) {
                    results.add(result)
                }
            } catch (Exception e) {
                log.error("Failed to generate thumbnail ${thumbDef.name}", e)
            }
        }

        return results
    }

    private ThumbnailingResult generateSingleThumbnail(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, ThumbDefinition thumbDef) throws IOException {
        ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.name)
        int size = thumbDef.maximumDimension
        Color backgroundColor = thumbDef.backgroundColor

        File workDir = new File(System.getProperty('java.io.tmpdir'))

        InputStream inputStream = imageBytes.openStream()
        try {
            List<String> args = []
            if (tool == 'vips') {
                args = buildVipsArgs(thumbDef, size, backgroundColor)
            } else if (tool == 'magick') {
                args = buildMagickArgs(thumbDef, size, backgroundColor)
            } else {
                throw new IllegalArgumentException("Unsupported tool: ${tool}")
            }

            OutputStream outputStream = destination.openStream()
            try {
                CommandExecutor.ExecResult result = commandExecutor.exec(tool == 'vips' ? 'vips' : 'magick', args, workDir, inputStream, timeoutSeconds, outputStream)

                if (result.exitCode != 0) {
                    log.warn("Tool ${tool} failed with exit code ${result.exitCode}: ${result.stderr}")
                    return null
                }

                // We don't know the exact dimensions without reading the file back,
                // so we'll return approximate dimensions based on the request
                return new ThumbnailingResult(size, size, thumbDef.square, thumbDef.name)
            } finally {
                outputStream.close()
            }
        } finally {
            inputStream.close()
        }
    }

    private List<String> buildVipsArgs(ThumbDefinition thumbDef, int size, Color backgroundColor) {
        List<String> args = ['thumbnail', 'stdin']

        if (thumbDef.square && thumbDef.centreCrop) {
            // Centre crop to square
            args.add('--size')
            args.add("${size}x${size}".toString())
            args.add('--crop')
            args.add('centre')
        } else if (thumbDef.square) {
            // Fit within square with background
            args.add('--size')
            args.add("${size}x${size}".toString())
            if (backgroundColor) {
                args.add('--background')
                args.add("${backgroundColor.red} ${backgroundColor.green} ${backgroundColor.blue}".toString())
            }
        } else if (size != -1 && thumbDef.width != -1) {
            args.add('--size')
            args.add("${thumbDef.width}x".toString())
        } else {
            args.add('--size')
            args.add("${size}x${size}".toString())
        }

        args.add('--intent')
        args.add('relative')
        args.add('-o')
        args.add('stdout')

        return args
    }

    private List<String> buildMagickArgs(ThumbDefinition thumbDef, int size, Color backgroundColor) {
        List<String> args = []
        args.add('stdin')
        args.add('-auto-orient')

        if (thumbDef.square && thumbDef.centreCrop) {
            // Centre crop to square
            args.add('-resize')
            args.add("${size}x${size}^".toString())
            args.add('-gravity')
            args.add('center')
            args.add('-extent')
            args.add("${size}x${size}".toString())
        } else if (thumbDef.square) {
            // Fit within square with background
            if (backgroundColor) {
                args.add('-background')
                args.add(String.format('#%02x%02x%02x', backgroundColor.red, backgroundColor.green, backgroundColor.blue))
            }
            args.add('-resize')
            args.add("${size}x${size}".toString())
            args.add('-gravity')
            args.add('center')
            args.add('-extent')
            args.add("${size}x${size}".toString())
        } else if (size != -1 && thumbDef.width != -1) {
            args.add('-resize')
            args.add("${thumbDef.width}x".toString())
        } else {
            args.add('-resize')
            args.add("${size}x${size}".toString())
        }

        args.add('-quality')
        args.add('85')
        args.add('-strip')
        args.add('stdout')

        return args
    }
}
