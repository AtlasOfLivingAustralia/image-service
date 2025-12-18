package au.org.ala.images.optimisation

import au.org.ala.images.ImageOptimisationService
import au.org.ala.images.util.ImageReaderUtils
import com.google.common.io.Files
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.imgscalr.Scalr

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.awt.Graphics2D
import java.awt.RenderingHints

@Slf4j
@CompileStatic
class ImageResizeTool implements ImageOptimTool {

    private Map toolConfig = [:]

    @Override
    void init(Map toolConfig) {
        this.toolConfig = toolConfig ?: [:]
    }

    @Override
    CommandExecutor.ExecResult run(File inputFile, File outputFile, Map stepConfig, Map context) {
        CommandExecutor.ExecResult res = new CommandExecutor.ExecResult(stdout: '', stderr: '')
        try {
            Map cfg = [:]
            cfg.putAll(toolConfig)
            if (stepConfig) cfg.putAll(stepConfig)

            int maxW = parseInt(cfg.maxWidth, 0)
            int maxH = parseInt(cfg.maxHeight, 0)
            if (maxW <= 0 && maxH <= 0) {
                // nothing to do; copy input to output
                copyFile(inputFile, outputFile)
                res.exitCode = 0
                return res
            }

            BufferedImage src
            int w
            int h
            ImageReaderUtils.withImageReader(Files.asByteSource(inputFile)) { reader ->
                w = reader.getWidth(0)
                h = reader.getHeight(0)
                src = reader.read(0)
            }

            try {
                if (w <= 0 || h <= 0) {
                    res.exitCode = 1
                    res.stderr = 'Unable to determine image dimensions'
                    return res
                }

                int targetW = w
                int targetH = h
                if (maxW > 0 || maxH > 0) {
                    double scaleW = maxW > 0 ? (double) maxW / (double) w : 1.0d
                    double scaleH = maxH > 0 ? (double) maxH / (double) h : 1.0d
                    double scale = Math.min(scaleW, scaleH)
                    if (scale < 1.0d) {
                        targetW = Math.max(1, (int) Math.floor(w * scale))
                        targetH = Math.max(1, (int) Math.floor(h * scale))
                    }
                }

                // If no downsizing, copy
                if (targetW == w && targetH == h) {
                    copyFile(inputFile, outputFile)
                    res.exitCode = 0
                    return res
                }

                if (src == null) {
                    res.exitCode = 1
                    res.stderr = 'Failed to read input image'
                    return res
                }

                boolean useScalr = "${stepConfig?.useScalr ?: false}"?.toBoolean()
                BufferedImage scaled
                if (useScalr) {
                    scaled = Scalr.resize(src, Scalr.Method.QUALITY, targetW, targetH)
                } else {
                    // High-quality Java2D scaling
                    scaled = new BufferedImage(targetW, targetH, bestTypeFor(src))
                    Graphics2D g = scaled.createGraphics()
                    try {
                        g.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BICUBIC)
                        g.setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
                        g.setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                        g.drawImage(src, 0, 0, targetW, targetH, null)
                    } finally {
                        g.dispose()
                    }
                }

                String format = (context?.format instanceof CharSequence) ? String.valueOf(context.format) : inferFormatFromName(outputFile?.name)
                if (stepConfig?.outputFormat instanceof CharSequence) {
                    format = String.valueOf(stepConfig.outputFormat)
                }
                if (!format) format = 'jpeg'

                outputFile.parentFile?.mkdirs()
                boolean ok = ImageIO.write(scaled, formatForImageIO(format), outputFile)
                if (!ok) {
                    res.exitCode = 1
                    res.stderr = 'ImageIO could not write format: ' + format
                    return res
                }
                res.exitCode = 0
                return res
            } finally {
                src?.flush()
            }
        } catch (Throwable e) {
            res.exitCode = 1
            res.stderr = e.message
            return res
        }
    }

    private static String inferFormatFromName(String name) {
        if (!name) return null
        String n = name.toLowerCase()
        return ImageOptimisationService.inferFormatForFilename(n)
    }

    private static String formatForImageIO(String fmt) {
        switch (String.valueOf(fmt)) {
            case 'jpeg': return 'jpg'
            default: return fmt
        }
    }

    private static int parseInt(Object v, int defVal) {
        if (v instanceof Number) return ((Number) v).intValue()
        if (v != null) try { return Integer.parseInt(String.valueOf(v)) } catch (Throwable ignore) {}
        return defVal
    }

    private static void copyFile(File src, File dst) {
        dst.parentFile?.mkdirs()
        try (def os = new FileOutputStream(dst);
             def is = new FileInputStream(src)) {
            is.transferTo(os)
        }
    }

    private static int bestTypeFor(BufferedImage src) {
        // Prefer to preserve alpha if present
        boolean hasAlpha = src.colorModel?.hasAlpha()
        return hasAlpha ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB
    }
}
