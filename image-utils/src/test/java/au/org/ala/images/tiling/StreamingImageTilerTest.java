package au.org.ala.images.tiling;

import au.org.ala.images.optimisation.CommandExecutor;
import com.google.common.io.ByteSink;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.awt.Color;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StreamingImageTilerTest {

    @Test
    public void tileImageFallsBackToVipsheaderWhenVipsHeaderActionIsUnavailable() throws Exception {
        RecordingCommandExecutor commandExecutor = new RecordingCommandExecutor("vipsheader");
        StreamingImageTiler tiler = new StreamingImageTiler(
            commandExecutor,
            "vips",
            new ImageTilerConfig(null, null, 256, 6, TileFormat.JPEG, Color.GRAY, false)
        );

        ImageTilerResults results = tiler.tileImage(
            new ByteArrayInputStream("fake-image".getBytes(StandardCharsets.UTF_8)),
            new DevNullSink(),
            0,
            0
        );

        assertTrue(results.getSuccess());
        assertEquals("vips", commandExecutor.invocations.get(0).command);
        assertEquals(Arrays.asList("header"), commandExecutor.invocations.get(0).args.subList(0, 1));
        assertEquals("vipsheader", commandExecutor.invocations.get(1).command);
        assertEquals(Arrays.asList("-f", "width"), commandExecutor.invocations.get(1).args.subList(0, 2));
        assertEquals("vipsheader", commandExecutor.invocations.get(2).command);
        assertEquals(Arrays.asList("-f", "height"), commandExecutor.invocations.get(2).args.subList(0, 2));
        assertTrue(commandExecutor.invocations.stream().anyMatch(inv -> "resize".equals(inv.args.get(0))));
        assertTrue(commandExecutor.invocations.stream().anyMatch(inv -> "extract_area".equals(inv.args.get(0))));
    }

    @Test
    public void tileImageUsesSiblingVipsheaderForCustomVipsPath() throws Exception {
        RecordingCommandExecutor commandExecutor = new RecordingCommandExecutor("/opt/libvips/bin/vipsheader");
        StreamingImageTiler tiler = new StreamingImageTiler(
            commandExecutor,
            "/opt/libvips/bin/vips",
            new ImageTilerConfig(null, null, 256, 6, TileFormat.JPEG, Color.GRAY, false)
        );

        ImageTilerResults results = tiler.tileImage(
            new ByteArrayInputStream("fake-image".getBytes(StandardCharsets.UTF_8)),
            new DevNullSink(),
            0,
            0
        );

        assertTrue(results.getSuccess());
        assertEquals("/opt/libvips/bin/vipsheader", commandExecutor.invocations.get(1).command);
        assertEquals("/opt/libvips/bin/vipsheader", commandExecutor.invocations.get(2).command);
    }

    @Test
    public void tileImageUsesNativePaddingPipelineForPngEdgeTiles() throws Exception {
        RecordingCommandExecutor commandExecutor = new RecordingCommandExecutor("vipsheader");
        StreamingImageTiler tiler = new StreamingImageTiler(
            commandExecutor,
            "vips",
            new ImageTilerConfig(null, null, 256, 6, TileFormat.PNG, Color.GRAY, true)
        );

        ImageTilerResults results = tiler.tileImage(
            new ByteArrayInputStream("fake-image".getBytes(StandardCharsets.UTF_8)),
            new DevNullSink(),
            0,
            0
        );

        assertTrue(results.getSuccess());
        assertTrue(commandExecutor.invocations.stream().anyMatch(inv -> "bandjoin_const".equals(inv.args.get(0))));
        assertTrue(commandExecutor.invocations.stream().anyMatch(inv -> "gravity".equals(inv.args.get(0))));
    }

    private static final class RecordingCommandExecutor implements CommandExecutor {
        private final String expectedVipsHeaderCommand;
        private final List<Invocation> invocations = new ArrayList<>();

        private RecordingCommandExecutor(String expectedVipsHeaderCommand) {
            this.expectedVipsHeaderCommand = expectedVipsHeaderCommand;
        }

        @Override
        public boolean isInstalled(String cmd) {
            return true;
        }

        @Override
        public ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds) {
            throw new UnsupportedOperationException("Not used in this test");
        }

        @Override
        public ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile) {
            throw new UnsupportedOperationException("Not used in this test");
        }

        @Override
        public ExecResult exec(String cmd, List<String> args, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream) {
            invocations.add(new Invocation(cmd, new ArrayList<>(args)));
            ExecResult res = new ExecResult();
            res.stdout = "";
            res.stderr = "";

            String action = args.isEmpty() ? "" : args.get(0);
            try {
                if (cmd.endsWith("vips") && "header".equals(action)) {
                    res.exitCode = 1;
                    res.stderr = "vips: unknown action \"header\"";
                    return res;
                }
                if (expectedVipsHeaderCommand.equals(cmd) && "-f".equals(action) && args.size() >= 2) {
                    res.exitCode = 0;
                    res.stdout = "width".equals(args.get(1)) ? "512\n" : "384\n";
                    return res;
                }
                if ("resize".equals(action)) {
                    res.exitCode = 0;
                    return res;
                }
                if ("extract_area".equals(action)) {
                    if (stdoutStream != null) {
                        stdoutStream.write("tile".getBytes(StandardCharsets.UTF_8));
                    }
                    res.exitCode = 0;
                    return res;
                }
                if ("bandjoin_const".equals(action)) {
                    if (stdoutStream != null) {
                        stdoutStream.write("alpha".getBytes(StandardCharsets.UTF_8));
                    }
                    res.exitCode = 0;
                    return res;
                }
                if ("gravity".equals(action)) {
                    if (stdoutStream != null) {
                        stdoutStream.write("padded".getBytes(StandardCharsets.UTF_8));
                    }
                    res.exitCode = 0;
                    return res;
                }
            } catch (IOException e) {
                res.exitCode = -1;
                res.stderr = e.getMessage();
                return res;
            }

            throw new AssertionError("Unexpected command: " + cmd + " " + args);
        }

        @Override
        public PipelineResult execPipeline(List<PipelineStage> stages, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream) {
            PipelineResult result = new PipelineResult();
            result.stageExitCodes = new ArrayList<>();
            result.stdout = "";
            result.stderr = "";
            try {
                for (PipelineStage stage : stages) {
                    invocations.add(new Invocation(stage.cmd, new ArrayList<>(stage.args)));
                    String action = stage.args.isEmpty() ? "" : stage.args.get(0);
                    if ("bandjoin_const".equals(action) || "gravity".equals(action) || "extract_area".equals(action)) {
                        result.stageExitCodes.add(0);
                        continue;
                    }
                    throw new AssertionError("Unexpected pipeline stage: " + stage.args);
                }
                if (stdoutStream != null) {
                    stdoutStream.write("padded".getBytes(StandardCharsets.UTF_8));
                }
                result.exitCode = 0;
                return result;
            } catch (IOException e) {
                result.exitCode = -1;
                result.stderr = e.getMessage();
                return result;
            }
        }
    }

    private static final class DevNullSink implements TilerSink {
        @Override
        public LevelSink getLevelSink(int level) {
            return (col, stripIndex, maxColsPerStrip) -> row -> new ByteSink() {
                @Override
                public OutputStream openStream() {
                    return new ByteArrayOutputStream();
                }
            };
        }
    }

    private static final class Invocation {
        private final String command;
        private final List<String> args;

        private Invocation(String command, List<String> args) {
            this.command = command;
            this.args = args;
        }
    }
}
