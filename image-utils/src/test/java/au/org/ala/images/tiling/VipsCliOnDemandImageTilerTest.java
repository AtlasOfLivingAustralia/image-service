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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class VipsCliOnDemandImageTilerTest {

    @Test
    public void generateTileUsesStructuredVipsCommandsForResizePipeline() {
        RecordingCommandExecutor commandExecutor = new RecordingCommandExecutor(false);
        RecordingFallbackTiler fallback = new RecordingFallbackTiler();
        VipsCliOnDemandImageTiler tiler = new VipsCliOnDemandImageTiler(
            commandExecutor,
            "vips",
            new ImageTilerConfig(Runnable::run, Runnable::run),
            fallback
        );
        CapturingTilerSink sink = new CapturingTilerSink();

        TileGenerationResult result = tiler.generateTile(
            new ByteArrayInputStream("fake-image".getBytes(StandardCharsets.UTF_8)),
            sink,
            0,
            0,
            0
        );

        assertTrue(result.isSuccess());
        assertEquals(0, fallback.invocationCount);
        assertArrayEquals("fake-image".getBytes(StandardCharsets.UTF_8), commandExecutor.extractInputBytes);
        assertArrayEquals("cropped".getBytes(StandardCharsets.UTF_8), commandExecutor.resizeInputBytes);
        assertEquals("resized", sink.getCapturedOutput());
        assertFalse(commandExecutor.invocations.stream().anyMatch(invocation -> "sh".equals(invocation.command)));
        assertEquals(1, commandExecutor.pipelineInvocations.size());
        assertFalse(commandExecutor.pipelineInvocations.get(0).stream().anyMatch(stage -> "sh".equals(stage.cmd)));

        CommandExecutor.PipelineStage extractStage = commandExecutor.findPipelineStage("extract_area");
        assertNotNull(extractStage);
        assertEquals("vips", extractStage.cmd);
        assertEquals(Arrays.asList("extract_area", "stdin", ".stdout.jpg", "0", "0", "500", "500"), extractStage.args);

        CommandExecutor.PipelineStage resizeStage = commandExecutor.findPipelineStage("resize");
        assertNotNull(resizeStage);
        assertEquals("vips", resizeStage.cmd);
        assertEquals(Arrays.asList("resize", "stdin", ".stdout.jpg", "0.5"), resizeStage.args);
    }

    @Test
    public void generateTileFallsBackWhenExtractStageFails() {
        RecordingCommandExecutor commandExecutor = new RecordingCommandExecutor(true);
        RecordingFallbackTiler fallback = new RecordingFallbackTiler();
        VipsCliOnDemandImageTiler tiler = new VipsCliOnDemandImageTiler(
            commandExecutor,
            "vips",
            new ImageTilerConfig(Runnable::run, Runnable::run),
            fallback
        );

        TileGenerationResult result = tiler.generateTile(
            new ByteArrayInputStream("fake-image".getBytes(StandardCharsets.UTF_8)),
            new CapturingTilerSink(),
            0,
            0,
            0
        );

        assertEquals(TileGenerationResult.Status.INTERNAL_ERROR, result.getStatus());
        assertEquals(1, fallback.invocationCount);
        assertFalse(commandExecutor.invocations.stream().anyMatch(invocation -> "sh".equals(invocation.command)));
        assertFalse(commandExecutor.pipelineInvocations.get(0).stream().anyMatch(stage -> "sh".equals(stage.cmd)));
    }

    private static final class RecordingCommandExecutor implements CommandExecutor {
        private final boolean failExtractStage;
        private final List<Invocation> invocations = new ArrayList<>();
        private final List<List<PipelineStage>> pipelineInvocations = new ArrayList<>();
        private byte[] extractInputBytes;
        private byte[] resizeInputBytes;

        private RecordingCommandExecutor(boolean failExtractStage) {
            this.failExtractStage = failExtractStage;
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
            String operation = args.get(0);
            ExecResult result = new ExecResult();
            result.stdout = "";
            result.stderr = "";

            try {
                byte[] inputBytes = stdinStream == null ? new byte[0] : stdinStream.readAllBytes();
                if ("header".equals(operation)) {
                    result.exitCode = 0;
                    result.stdout = "500x500";
                    return result;
                }
                if ("extract_area".equals(operation)) {
                    extractInputBytes = inputBytes;
                    if (failExtractStage) {
                        result.exitCode = 9;
                        result.stderr = "extract failed";
                        return result;
                    }
                    stdoutStream.write("cropped".getBytes(StandardCharsets.UTF_8));
                    result.exitCode = 0;
                    return result;
                }
                if ("resize".equals(operation)) {
                    resizeInputBytes = inputBytes;
                    stdoutStream.write("resized".getBytes(StandardCharsets.UTF_8));
                    result.exitCode = 0;
                    return result;
                }
                throw new AssertionError("Unexpected command operation: " + operation);
            } catch (IOException e) {
                result.exitCode = -1;
                result.stderr = e.getMessage();
                return result;
            }
        }

        @Override
        public PipelineResult execPipeline(List<PipelineStage> stages, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream) {
            pipelineInvocations.add(stages);
            PipelineResult result = new PipelineResult();
            result.stageExitCodes = Arrays.asList(0, 0);
            result.stdout = "";
            result.stderr = "";
            try {
                extractInputBytes = stdinStream == null ? new byte[0] : stdinStream.readAllBytes();
                CommandExecutor.PipelineStage extractStage = stages.get(0);
                CommandExecutor.PipelineStage resizeStage = stages.get(1);
                if (!"extract_area".equals(extractStage.args.get(0)) || !"resize".equals(resizeStage.args.get(0))) {
                    throw new AssertionError("Unexpected pipeline stages: " + stages);
                }

                if (failExtractStage) {
                    result.exitCode = 9;
                    result.failedStageIndex = 0;
                    result.stageExitCodes = Arrays.asList(9, 0);
                    result.stderr = "stage 0: extract failed";
                    return result;
                }

                resizeInputBytes = "cropped".getBytes(StandardCharsets.UTF_8);
                stdoutStream.write("resized".getBytes(StandardCharsets.UTF_8));
                result.exitCode = 0;
                return result;
            } catch (IOException e) {
                result.exitCode = -1;
                result.stderr = e.getMessage();
                return result;
            }
        }

        private PipelineStage findPipelineStage(String operation) {
            for (List<PipelineStage> invocation : pipelineInvocations) {
                for (PipelineStage stage : invocation) {
                    if (stage.args != null && !stage.args.isEmpty() && operation.equals(stage.args.get(0))) {
                        return stage;
                    }
                }
            }
            return null;
        }
    }

    private static final class RecordingFallbackTiler implements IOnDemandImageTiler {
        private int invocationCount;

        @Override
        public TileGenerationResult generateTile(InputStream imageInputStream, TilerSink tilerSink, int level, int x, int y) {
            invocationCount++;
            return TileGenerationResult.internalError("fallback invoked");
        }
    }

    private static final class CapturingTilerSink implements TilerSink {
        private final ByteArrayOutputStream output = new ByteArrayOutputStream();

        @Override
        public LevelSink getLevelSink(int level) {
            return (col, stripIndex, maxColsPerStrip) -> row -> new ByteSink() {
                @Override
                public OutputStream openStream() {
                    return output;
                }
            };
        }

        private String getCapturedOutput() {
            return output.toString(StandardCharsets.UTF_8);
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



