package au.org.ala.images.optimisation


import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import org.apache.commons.io.IOUtils

import java.nio.charset.Charset
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

@Slf4j
@CompileStatic
class ProcessCommandExecutor implements CommandExecutor {

    @Override
    boolean isInstalled(String cmd) {
        // Try to find the command on PATH
        String path = System.getenv('PATH') ?: ''
        String sep = File.pathSeparator
        for (String p : path.split(Pattern.quote(sep))) {
            if (!p) continue
            File f = new File(p, cmd)
            if (f.exists() && f.canExecute()) return true
            // Also check Windows extensions
            if (System.getProperty('os.name').toLowerCase().contains('win')) {
                for (String ext : ['.exe', '.bat', '.cmd']) {
                    File wf = new File(p, cmd + ext)
                    if (wf.exists() && wf.canExecute()) return true
                }
            }
        }
        return false
    }

    @Override
    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds) {
        return exec(cmd, args, workingDir, env, timeoutSeconds, null)
    }

    @Override
    ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds,
                    File stdinFile) {
        List<String> command = new ArrayList<>()
        command.add(cmd)
        if (args) command.addAll(args)
        ProcessBuilder pb = new ProcessBuilder(command)
        if (workingDir) pb.directory(workingDir)
        if (env) pb.environment().putAll(env)
        pb.redirectErrorStream(false)

        Process process
        try {
            process = pb.start()
        } catch (IOException e) {
            ExecResult r = new ExecResult()
            r.exitCode = -1
            r.stdout = ''
            r.stderr = 'Failed to start process: ' + e.message
            return r
        }

        def pool = Executors.newFixedThreadPool(2)
        StringBuilder err = new StringBuilder()

        // Capture stdout as bytes or text depending on flag
        StringBuilder out = null
        def outTask
        if (stdinFile) {
//            outBytes = new ByteArrayOutputStream()
            stdinFile.parentFile?.mkdirs()
            outTask = pool.submit({
                stdinFile.withOutputStream {
                    IOUtils.copy(process.inputStream, it)
                }
//                byte[] buffer = new byte[8192]
//                InputStream is = process.inputStream
//                int read
//                while ((read = is.read(buffer)) != -1) {
//                    outBytes.write(buffer, 0, read)
//                }
            } as Runnable)
        } else {
            out = new StringBuilder()
            outTask = pool.submit({ out.append(process.inputStream.getText(Charset.defaultCharset().name())) } as Runnable)
        }
        def errTask = pool.submit({ err.append(process.errorStream.getText(Charset.defaultCharset().name())) } as Runnable)

        boolean finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS)
        if (!finished) {
            process.destroyForcibly()
            outTask.cancel(true)
            errTask.cancel(true)
            pool.shutdownNow()
            ExecResult r = new ExecResult()
            r.exitCode = -1
            r.stdout = out?.toString() ?: ''
//            r.stdoutBytes = outBytes?.toByteArray()
            r.stderr = 'Timed out after ' + timeoutSeconds + 's'
            return r
        }
        outTask.get(1, TimeUnit.SECONDS)
        errTask.get(1, TimeUnit.SECONDS)
        pool.shutdown()

        ExecResult res = new ExecResult()
        res.exitCode = process.exitValue()
        res.stdout = out?.toString() ?: ''
//        res.stdoutBytes = outBytes?.toByteArray()
        res.stderr = err.toString()
        return res
    }
}
