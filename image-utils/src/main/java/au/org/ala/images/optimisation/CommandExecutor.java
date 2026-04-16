package au.org.ala.images.optimisation;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;

public interface CommandExecutor {

    static class ExecResult {
        public int exitCode;
        public String stdout;
        public String stderr;
    }

    boolean isInstalled(String cmd);

    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds);

    /**
     * Execute command with option to capture stdout as raw bytes (for binary output).
     * @param captureStdoutAsBytes if true, stdout will be captured as bytes instead of text
     */
    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile);

    /**
     * Execute command with stdin from InputStream and stdout to OutputStream.
     * @param stdinStream stream to pipe to command's stdin (can be null)
     * @param stdoutStream stream to write command's stdout to (can be null)
     */
    ExecResult exec(String cmd, List<String> args, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream);
}


