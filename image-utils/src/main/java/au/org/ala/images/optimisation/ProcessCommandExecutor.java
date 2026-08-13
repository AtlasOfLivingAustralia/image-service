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
import java.util.Collections;
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

    @Override
    public PipelineResult execPipeline(List<PipelineStage> stages,
                                       File workingDir,
                                       InputStream stdinStream,
                                       long timeoutSeconds,
                                       OutputStream stdoutStream) {
        PipelineResult result = new PipelineResult();
        result.stdout = "";
        result.stderr = "";

        if (stages == null || stages.isEmpty()) {
            result.exitCode = -1;
            result.stderr = "No pipeline stages provided";
            result.stageExitCodes = Collections.emptyList();
            return result;
        }

        List<ProcessBuilder> builders = new ArrayList<>();
        for (PipelineStage stage : stages) {
            List<String> command = new ArrayList<>();
            command.add(stage.cmd);
            if (stage.args != null) {
                command.addAll(stage.args);
            }
            ProcessBuilder pb = new ProcessBuilder(command);
            if (workingDir != null) {
                pb.directory(workingDir);
            }
            pb.redirectErrorStream(false);
            builders.add(pb);
        }

        List<Process> processes;
        try {
            processes = ProcessBuilder.startPipeline(builders);
        } catch (IOException e) {
            result.exitCode = -1;
            result.stderr = "Failed to start pipeline: " + e.getMessage();
            result.stageExitCodes = Collections.emptyList();
            return result;
        }

        ExecutorService pool = Executors.newFixedThreadPool(stages.size() + 2);
        List<Future<String>> stderrTasks = new ArrayList<>();
        for (Process process : processes) {
            stderrTasks.add(pool.submit(() -> {
                try {
                    return IOUtils.toString(process.getErrorStream(), Charset.defaultCharset());
                } catch (IOException e) {
                    log.error("Error reading process error stream", e);
                    return "";
                }
            }));
        }

        Process first = processes.get(0);
        Process last = processes.get(processes.size() - 1);
        StringBuilder stdoutCapture = new StringBuilder();

        Future<?> stdinTask = pool.submit(() -> {
            try (OutputStream processStdin = first.getOutputStream()) {
                if (stdinStream != null) {
                    IOUtils.copy(stdinStream, processStdin);
                }
            } catch (IOException e) {
                log.debug("Error writing pipeline stdin: {}", e.getMessage());
            }
        });

        Future<?> stdoutTask = pool.submit(() -> {
            try (InputStream processStdout = last.getInputStream()) {
                if (stdoutStream != null) {
                    IOUtils.copy(processStdout, stdoutStream);
                } else {
                    stdoutCapture.append(IOUtils.toString(processStdout, Charset.defaultCharset()));
                }
            } catch (IOException e) {
                log.error("Error reading pipeline stdout", e);
            }
        });

        long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        List<Integer> stageExitCodes = new ArrayList<>();

        try {
            for (Process process : processes) {
                long remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0 || !process.waitFor(remainingNanos, TimeUnit.NANOSECONDS)) {
                    destroyProcesses(processes);
                    stdinTask.cancel(true);
                    stdoutTask.cancel(true);
                    stderrTasks.forEach(task -> task.cancel(true));
                    result.exitCode = -1;
                    result.stderr = "Timed out after " + timeoutSeconds + "s";
                    result.stdout = stdoutCapture.toString();
                    result.stageExitCodes = stageExitCodes;
                    return result;
                }
                stageExitCodes.add(process.exitValue());
            }

            stdinTask.get(1, TimeUnit.SECONDS);
            stdoutTask.get(1, TimeUnit.SECONDS);

            StringBuilder mergedErr = new StringBuilder();
            int failedStage = -1;
            for (int i = 0; i < stderrTasks.size(); i++) {
                String stageErr = stderrTasks.get(i).get(1, TimeUnit.SECONDS);
                if (stageErr != null && !stageErr.isEmpty()) {
                    if (mergedErr.length() > 0) {
                        mergedErr.append(" | ");
                    }
                    mergedErr.append("stage ").append(i).append(": ").append(stageErr);
                }
                if (failedStage == -1 && i < stageExitCodes.size() && stageExitCodes.get(i) != 0) {
                    failedStage = i;
                }
            }

            result.failedStageIndex = failedStage;
            result.stageExitCodes = stageExitCodes;
            result.exitCode = failedStage >= 0 ? stageExitCodes.get(failedStage) : 0;
            result.stdout = stdoutCapture.toString();
            result.stderr = mergedErr.toString();
            return result;
        } catch (Exception e) {
            destroyProcesses(processes);
            result.exitCode = -1;
            result.stderr = "Pipeline execution failed: " + e.getMessage();
            result.stdout = stdoutCapture.toString();
            result.stageExitCodes = stageExitCodes;
            return result;
        } finally {
            pool.shutdownNow();
        }
    }

    private void destroyProcesses(List<Process> processes) {
        for (Process process : processes) {
            try {
                if (process.isAlive()) {
                    process.destroyForcibly();
                }
            } catch (Exception ignored) {
            }
        }
    }
}
