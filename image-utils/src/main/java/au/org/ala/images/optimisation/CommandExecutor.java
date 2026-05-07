package au.org.ala.images.optimisation;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface CommandExecutor {

    class ExecResult {
        public int exitCode;
        public String stdout;
        public String stderr;
    }

    class PipelineStage {
        public final String cmd;
        public final List<String> args;

        public PipelineStage(String cmd, List<String> args) {
            this.cmd = cmd;
            this.args = args;
        }
    }

    class PipelineResult extends ExecResult {
        public int failedStageIndex = -1;
        public List<Integer> stageExitCodes;
    }

    boolean isInstalled(String cmd);

    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds);

    /**
     * Execute command with option to capture stdout as raw bytes (for binary output).
     * @param stdinFile optional file to read stdin bytes from
     */
    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile);

    /**
     * Execute command with stdin from InputStream and stdout to OutputStream.
     * @param stdinStream stream to pipe to command's stdin (can be null)
     * @param stdoutStream stream to write command's stdout to (can be null)
     */
    ExecResult exec(String cmd, List<String> args, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream);

    /**
     * Execute a shell-free command pipeline where stdout from each stage feeds stdin of the next stage.
     * stdinStream is piped to the first stage and stdoutStream receives output from the final stage.
     */
    default PipelineResult execPipeline(List<PipelineStage> stages,
                                        File workingDir,
                                        InputStream stdinStream,
                                        long timeoutSeconds,
                                        OutputStream stdoutStream) {
        throw new UnsupportedOperationException("Pipeline execution is not supported by this executor");
    }
}


