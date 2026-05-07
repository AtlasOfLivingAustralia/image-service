package au.org.ala.images.thumb;

import au.org.ala.images.optimisation.CommandExecutor;
import au.org.ala.images.util.ByteSinkFactory;
import com.google.common.io.ByteSink;
import com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Streaming thumbnailer that uses external tools (vips, imagemagick) to generate thumbnails
 * without loading the full image into memory. Streams bytes directly from stdin to stdout.
 */
public class StreamingImageThumbnailer implements IImageThumbnailer {

    private static final Logger log = LoggerFactory.getLogger(StreamingImageThumbnailer.class);

    private final CommandExecutor commandExecutor;
    private final String tool; // 'vips' or 'magick'
    private final long timeoutSeconds;

    public StreamingImageThumbnailer(CommandExecutor commandExecutor) {
        this(commandExecutor, "vips", 30);
    }

    public StreamingImageThumbnailer(CommandExecutor commandExecutor, String tool) {
        this(commandExecutor, tool, 30);
    }

    public StreamingImageThumbnailer(CommandExecutor commandExecutor, String tool, long timeoutSeconds) {
        this.commandExecutor = commandExecutor;
        this.tool = tool;
        this.timeoutSeconds = timeoutSeconds;
    }

    @Override
    public List<ThumbnailingResult> generateThumbnails(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, List<ThumbDefinition> thumbDefs) throws IOException {
        List<ThumbnailingResult> results = new ArrayList<>();

        for (ThumbDefinition thumbDef : thumbDefs) {
            try {
                ThumbnailingResult result = generateSingleThumbnail(imageBytes, byteSinkFactory, thumbDef);
                if (result != null) {
                    results.add(result);
                }
            } catch (Exception e) {
                log.error("Failed to generate thumbnail {}", thumbDef.getName(), e);
            }
        }

        return results;
    }

    private ThumbnailingResult generateSingleThumbnail(ByteSource imageBytes, ByteSinkFactory byteSinkFactory, ThumbDefinition thumbDef) throws IOException {
        ByteSink destination = byteSinkFactory.getByteSinkForNames(thumbDef.getName());
        int size = thumbDef.getMaximumDimension();
        Color backgroundColor = thumbDef.getBackgroundColor();

        File workDir = new File(System.getProperty("java.io.tmpdir"));

        try (InputStream inputStream = imageBytes.openStream()) {
            List<String> args;
            if ("vips".equals(tool)) {
                args = buildVipsArgs(thumbDef, size, backgroundColor);
            } else if ("magick".equals(tool)) {
                args = buildMagickArgs(thumbDef, size, backgroundColor);
            } else {
                throw new IllegalArgumentException("Unsupported tool: " + tool);
            }

            try (OutputStream outputStream = destination.openStream()) {
                CommandExecutor.ExecResult result = commandExecutor.exec("vips".equals(tool) ? "vips" : "magick", args, workDir, inputStream, timeoutSeconds, outputStream);

                if (result.exitCode != 0) {
                    log.warn("Tool {} failed with exit code {}: {}", tool, result.exitCode, result.stderr);
                    return null;
                }

                // We don't know the exact dimensions without reading the file back,
                // so we'll return approximate dimensions based on the request
                return new ThumbnailingResult(size, size, thumbDef.isSquare(), thumbDef.getName());
            }
        }
    }

    private List<String> buildVipsArgs(ThumbDefinition thumbDef, int size, Color backgroundColor) {
        List<String> args = new ArrayList<>();
        args.add("thumbnail");
        args.add("stdin");

        if (thumbDef.isSquare() && thumbDef.isCentreCrop()) {
            // Centre crop to square
            args.add("--size");
            args.add(size + "x" + size);
            args.add("--crop");
            args.add("centre");
        } else if (thumbDef.isSquare()) {
            // Fit within square with background
            args.add("--size");
            args.add(size + "x" + size);
            if (backgroundColor != null) {
                args.add("--background");
                args.add(backgroundColor.getRed() + " " + backgroundColor.getGreen() + " " + backgroundColor.getBlue());
            }
        } else if (size != -1 && thumbDef.getWidth() != -1) {
            args.add("--size");
            args.add(thumbDef.getWidth() + "x");
        } else {
            args.add("--size");
            args.add(size + "x" + size);
        }

        args.add("--intent");
        args.add("relative");
        args.add("-o");
        args.add("stdout");

        return args;
    }

    private List<String> buildMagickArgs(ThumbDefinition thumbDef, int size, Color backgroundColor) {
        List<String> args = new ArrayList<>();
        args.add("stdin");
        args.add("-auto-orient");

        if (thumbDef.isSquare() && thumbDef.isCentreCrop()) {
            // Centre crop to square
            args.add("-resize");
            args.add(size + "x" + size + "^");
            args.add("-gravity");
            args.add("center");
            args.add("-extent");
            args.add(size + "x" + size);
        } else if (thumbDef.isSquare()) {
            // Fit within square with background
            if (backgroundColor != null) {
                args.add("-background");
                args.add(String.format("#%02x%02x%02x", backgroundColor.getRed(), backgroundColor.getGreen(), backgroundColor.getBlue()));
            }
            args.add("-resize");
            args.add(size + "x" + size);
            args.add("-gravity");
            args.add("center");
            args.add("-extent");
            args.add(size + "x" + size);
        } else if (size != -1 && thumbDef.getWidth() != -1) {
            args.add("-resize");
            args.add(thumbDef.getWidth() + "x");
        } else {
            args.add("-resize");
            args.add(size + "x" + size);
        }

        args.add("-quality");
        args.add("85");
        args.add("-strip");
        args.add("stdout");

        return args;
    }
}
