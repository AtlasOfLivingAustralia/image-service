package au.org.ala.images.iiif;

import au.org.ala.images.optimisation.CommandExecutor;
import com.google.common.io.ByteSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * CLI-based IIIF Image API 3.0 style processor using ImageMagick (magick) or libvips (vips).
 * Currently only ImageMagick is fully implemented for IIIF chaining.
 */
public class StreamingIiifImageProcessor implements IiifImageProcessor {

    private static final Logger log = LoggerFactory.getLogger(StreamingIiifImageProcessor.class);
    private final CommandExecutor commandExecutor;
    private final String tool;

    public StreamingIiifImageProcessor(CommandExecutor commandExecutor) {
        this(commandExecutor, "magick");
    }

    public StreamingIiifImageProcessor(CommandExecutor commandExecutor, String tool) {
        this.commandExecutor = commandExecutor;
        this.tool = tool;
    }

    @Override
    public Result process(ByteSource imageBytes, Region region, Size size, Rotation rotation, Quality quality, Format format, OutputStream out) throws IOException {

        if ("vips".equals(tool)) {
            // Vips CLI is more complex for chaining.
            throw new UnsupportedOperationException("Vips CLI for IIIF processor not yet implemented. Use magick.");
        }

        List<String> args = new ArrayList<>();
        // Use magick's ability to chain operations
        // magick - [ops] format:-
        args.add("-");

        // 1. Region
        switch (region.type) {
            case SQUARE:
                args.add("-gravity");
                args.add("Center");
                args.add("-crop");
                args.add("%[fx:min(w,h)]x%[fx:min(w,h)]+0+0");
                args.add("+repage");
                break;
            case ABSOLUTE:
                args.add("-crop");
                args.add(String.format("%dx%d+%d+%d", (int) region.w, (int) region.h, (int) region.x, (int) region.y));
                args.add("+repage");
                break;
            case PERCENT:
                args.add("-crop");
                args.add(String.format("%s%%x%s%%+%s%%+%s%%", IiifImageProcessor.fmt(region.w), IiifImageProcessor.fmt(region.h), IiifImageProcessor.fmt(region.x), IiifImageProcessor.fmt(region.y)));
                args.add("+repage");
                break;
            case ASPECT:
                // Tricky with CLI without knowing dimensions, skipping for now
                break;
            case FULL:
            default:
                break;
        }

        // 2. Size
        switch (size.type) {
            case WIDTH_ONLY:
                args.add("-resize");
                args.add(size.w + (size.upscaling ? "" : ">"));
                break;
            case HEIGHT_ONLY:
                args.add("-resize");
                args.add("x" + size.h + (size.upscaling ? "" : ">"));
                break;
            case PERCENT:
                args.add("-resize");
                args.add(IiifImageProcessor.fmt(size.percent) + "%");
                break;
            case EXACT:
                args.add("-resize");
                args.add(String.format("%dx%d!", size.w, size.h));
                break;
            case BEST_FIT:
                args.add("-resize");
                args.add(String.format("%dx%d%s", size.w, size.h, size.upscaling ? "" : ">"));
                break;
            case MAX:
                break;
        }

        // 3. Rotation & Mirroring
        if (rotation.mirror) {
            args.add("-flop");
        }
        if (rotation.degrees != 0) {
            args.add("-rotate");
            args.add(IiifImageProcessor.fmt(rotation.degrees));
        }

        // 4. Quality
        switch (quality) {
            case GRAY:
                args.add("-colorspace");
                args.add("Gray");
                break;
            case BITONAL:
                args.add("-threshold");
                args.add("50%");
                args.add("-colorspace");
                args.add("Gray");
                break;
            case COLOR:
            case DEFAULT:
            default:
                break;
        }

        // 5. Format
        String outFormat = format.getFormatName();
        if ("jpg".equals(outFormat)) outFormat = "jpeg";
        args.add(outFormat + ":-");

        try (InputStream is = imageBytes.openStream()) {
            CommandExecutor.ExecResult result = commandExecutor.exec(tool, args, null, is, 60, out);
            if (result.exitCode != 0) {
                throw new IOException(tool + " failed with exit code " + result.exitCode + ": " + result.stderr);
            }
        }

        return new Result(0, 0, format.getMimeType());
    }
}
