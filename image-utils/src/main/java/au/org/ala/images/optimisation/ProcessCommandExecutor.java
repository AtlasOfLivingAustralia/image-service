package au.org.ala.images.optimisation;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class ProcessCommandExecutor implements CommandExecutor {

    private static final Logger log = LoggerFactory.getLogger(ProcessCommandExecutor.class);

    @Override
    public boolean isInstalled(String cmd) {
        // Try to find the command on PATH
        String path = System.getenv("PATH");
        if (path == null) path = "";
        String sep = File.pathSeparator;
        for (String p : path.split(Pattern.quote(sep))) {
            if (p.isEmpty()) continue;
            File f = new File(p, cmd);
            if (f.exists() && f.canExecute()) return true;
            // Also check Windows extensions
            if (System.getProperty("os.name").toLowerCase().contains("win")) {
                for (String ext : new String[]{".exe", ".bat", ".cmd"}) {
                    File wf = new File(p, cmd + ext);
                    if (wf.exists() && wf.canExecute()) return true;
                }
            }
        }
        return false;
    }

    @Override
    public ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds) {
        return exec(cmd, args, workingDir, env, timeoutSeconds, null);
    }

    @Override
    public ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds,
                    File stdinFile) {
        List<String> command = new ArrayList<>();
        command.add(cmd);
        if (args != null) command.addAll(args);
        ProcessBuilder pb = new ProcessBuilder(command);
        if (workingDir != null) pb.directory(workingDir);
        if (env != null) pb.environment().putAll(env);
        pb.redirectErrorStream(false);

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            ExecResult r = new ExecResult();
            r.exitCode = -1;
            r.stdout = "";
            r.stderr = "Failed to start process: " + e.getMessage();
            return r;
        }

        ExecutorService pool = Executors.newFixedThreadPool(2);
        StringBuilder err = new StringBuilder();
        StringBuilder out = new StringBuilder();
        
        Future<?> outTask;
        if (stdinFile != null) {
            outTask = pool.submit(() -> {
                try (OutputStream os = new java.io.FileOutputStream(stdinFile)) {
                    IOUtils.copy(process.getInputStream(), os);
                } catch (IOException e) {
                    log.error("Error copying process output to file", e);
                }
            });
        } else {
            outTask = pool.submit(() -> {
                try {
                    out.append(IOUtils.toString(process.getInputStream(), Charset.defaultCharset()));
                } catch (IOException e) {
                    log.error("Error reading process output", e);
                }
            });
        }
        
        Future<?> errTask = pool.submit(() -> {
            try {
                err.append(IOUtils.toString(process.getErrorStream(), Charset.defaultCharset()));
            } catch (IOException e) {
                log.error("Error reading process error stream", e);
            }
        });

        try {
            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                outTask.cancel(true);
                errTask.cancel(true);
                pool.shutdownNow();
                ExecResult r = new ExecResult();
                r.exitCode = -1;
                r.stdout = out.toString();
                r.stderr = "Timed out after " + timeoutSeconds + "s";
                return r;
            }
            outTask.get(1, TimeUnit.SECONDS);
            errTask.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Error waiting for process", e);
        } finally {
            pool.shutdown();
        }

        ExecResult res = new ExecResult();
        res.exitCode = process.exitValue();
        res.stdout = out.toString();
        res.stderr = err.toString();
        return res;
    }

    @Override
    public ExecResult exec(String cmd, List<String> args, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream) {
        List<String> command = new ArrayList<>();
        command.add(cmd);
        if (args != null) command.addAll(args);
        ProcessBuilder pb = new ProcessBuilder(command);
        if (workingDir != null) pb.directory(workingDir);
        pb.redirectErrorStream(false);

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            ExecResult r = new ExecResult();
            r.exitCode = -1;
            r.stdout = "";
            r.stderr = "Failed to start process: " + e.getMessage();
            return r;
        }

        ExecutorService pool = Executors.newFixedThreadPool(3);
        StringBuilder err = new StringBuilder();
        StringBuilder out = new StringBuilder();

        // Pipe stdin to process if provided
        Future<?> stdinTask = null;
        if (stdinStream != null) {
            stdinTask = pool.submit(() -> {
                try (OutputStream os = process.getOutputStream()) {
                    IOUtils.copy(stdinStream, os);
                } catch (IOException e) {
                    log.debug("Error writing to stdin: {}", e.getMessage());
                }
            });
        } else {
            try {
                process.getOutputStream().close();
            } catch (IOException ignored) {}
        }

        // Capture stdout to stream or discard
        Future<?> outTask = pool.submit(() -> {
            try {
                if (stdoutStream != null) {
                    IOUtils.copy(process.getInputStream(), stdoutStream);
                } else {
                    out.append(IOUtils.toString(process.getInputStream(), Charset.defaultCharset()));
                }
            } catch (IOException e) {
                log.error("Error reading process output", e);
            }
        });

        Future<?> errTask = pool.submit(() -> {
            try {
                err.append(IOUtils.toString(process.getErrorStream(), Charset.defaultCharset()));
            } catch (IOException e) {
                log.error("Error reading process error stream", e);
            }
        });

        try {
            boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                if (stdinTask != null) stdinTask.cancel(true);
                outTask.cancel(true);
                errTask.cancel(true);
                pool.shutdownNow();
                ExecResult r = new ExecResult();
                r.exitCode = -1;
                r.stdout = out.toString();
                r.stderr = "Timed out after " + timeoutSeconds + "s";
                return r;
            }

            if (stdinTask != null) stdinTask.get(1, TimeUnit.SECONDS);
            outTask.get(1, TimeUnit.SECONDS);
            errTask.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Error waiting for process", e);
        } finally {
            pool.shutdown();
        }

        ExecResult res = new ExecResult();
        res.exitCode = process.exitValue();
        res.stdout = out.toString();
        res.stderr = err.toString();
        return res;
    }
}
