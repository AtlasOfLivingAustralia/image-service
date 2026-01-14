package au.org.ala.images.optimisation

import groovy.transform.CompileStatic

@CompileStatic
interface CommandExecutor {

    static class ExecResult {
        int exitCode
        String stdout
        String stderr
    }

    boolean isInstalled(String cmd)

    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds)

    /**
     * Execute command with option to capture stdout as raw bytes (for binary output).
     * @param captureStdoutAsBytes if true, stdout will be captured as bytes instead of text
     */
    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile)
}


