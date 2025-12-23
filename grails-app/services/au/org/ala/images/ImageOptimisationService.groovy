package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.config.Stage
import au.org.ala.images.config.StageStep
import au.org.ala.images.config.Tool
import au.org.ala.images.config.Filter

import au.org.ala.images.metrics.MetricsSupport
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ImageOptimTool
import au.org.ala.images.optimisation.ProcessCommandExecutor
import com.google.common.io.Files
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import groovy.transform.CompileDynamic
import au.org.ala.images.util.ImageReaderUtils
import org.apache.commons.io.FilenameUtils
import org.apache.tika.mime.MimeType
import org.apache.tika.mime.MimeTypes
import org.springframework.beans.factory.annotation.Autowired

import javax.imageio.ImageReader
import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.function.Function

import static java.nio.file.Files.*

@Slf4j
@CompileStatic
class ImageOptimisationService implements MetricsSupport {

    private static final String TOKEN_IN = '%IN%'
    private static final String TOKEN_OUT = '%OUT%'
    private static final String TOKEN_QUALITY = '%QUALITY%'
    private static final String TOKEN_QUALITY_RANGE = '%QUALITY_RANGE%'

    def grailsApplication
    @Autowired
    ImageOptimisationConfig imageOptimisationConfig
    CommandExecutor commandExecutor = new ProcessCommandExecutor()
    // Cache for Java SPI tools by tool key
    private final Map<String, ImageOptimTool> javaTools = new HashMap<>()

    static class OptimisationResult {
        File optimisedFile
        long originalBytes
        long optimisedBytes
        List<String> warnings = []
        // Aggregated counts by message
        Map<String, Integer> warningsSummary = [:]
        boolean skipped
        String outputContentType
    }

    @CompileDynamic
    OptimisationResult optimise(File inputFile, String contentType, String pipelineName = 'default') {
        OptimisationResult result = new OptimisationResult(
                optimisedFile: inputFile,
                originalBytes: inputFile.length(),
                optimisedBytes: inputFile.length(),
                skipped: false
        )

        if (!imageOptimisationConfig.enabled) {
            log.debug('Image optimisation disabled via config')
            return result
        }

        long skipThreshold = imageOptimisationConfig.skipThresholdBytes
        if (inputFile.length() <= skipThreshold) {
            log.debug('Skipping optimisation: file {} length {} <= threshold {}', inputFile, inputFile.length(), skipThreshold)
            result.skipped = true
            return result
        }

        String format = inferFormat(contentType, inputFile)

        // Staged pipeline (only mode supported)
        List<Stage> stages = imageOptimisationConfig.stages[pipelineName]
        if (stages) {
            // prepare work dir
            File workDir = new File(imageOptimisationConfig.tempDir, 'imgopt-' + UUID.randomUUID().toString())
            workDir.mkdirs()
            File current = new File(workDir, 'current' + extensionFor(format, inputFile))
            Files.copy(inputFile, current)

            long timeoutSeconds = imageOptimisationConfig.timeoutSecondsPerTool
            int maxLog = imageOptimisationConfig.maxLogChars

            for (Stage stage : stages) {
                if (!stage) continue
                String stageName = stage.name ?: 'stage'
                Filter filter = stage.filter
                if (!passesFilter(current, contentType, format, filter)) {
                    log.debug('Stage {} skipped by filter {} for file {}', stageName, filter, current)
                    continue
                }
                // Determine steps: inline 'steps' or reference a toolset by name
                List<StageStep> steps = stage.steps
                if (!steps) {
                    String ref = stage.toolsRef
                    steps = imageOptimisationConfig.getToolsetSteps(ref, format)
                }
                if (!steps) {
                    log.debug('Stage {} has no steps/toolset; skipping', stageName)
                    continue
                }

                boolean allowLossy = stage.allowLossy

                long beforeStage = current.length()
                for (int i = 0; i < steps.size(); i++) {
                    StageStep step = steps[i]
                    String toolName = step.tool
                    if (!toolName) continue
                    boolean stepLossy = step.lossy
                    if (stepLossy && !allowLossy) {
                        log.debug('Skipping lossy step {} in stage {} because stage does not allow lossy', toolName, stageName)
                        continue
                    }

                    Tool tool = imageOptimisationConfig.tools[toolName]
                    if (!tool) {
                        String msg = "Tool '${toolName}' not configured"
                        log.warn(msg)
                        addWarning(result, msg)
                        continue
                    }
                    // Resolve tool with fallback chain; supports both process and java tools
                    Tool resolved = resolveAnyToolWithFallback(toolName, imageOptimisationConfig.tools)
                    if (resolved == null) {
                        String msg = "No available implementation found for tool '${toolName}' (including fallbacks) — skipping"
                        log.warn(msg)
                        addWarning(result, msg)
                        continue
                    }
                    String type = resolved.type
                    boolean inPlace = resolved.inPlace
                    boolean stdout = resolved.stdout

                    // Support cross-format conversion when a step declares outputFormat
                    String targetFormat = step.outputFormat
                    String outExt = (!inPlace && !stdout) ? extensionFor(targetFormat ?: format, current) : null

                    boolean acceptLarger = step.acceptLarger
                    boolean alwaysAccept = step.alwaysAccept

                    List<String> confArgs = step.args
                    Map<String, String> tokens = buildTokens(current, workDir, format, [:], outExt)
                    List<String> baseArgs = confArgs.collect { substitute(it, tokens) }
//                    List<String> baseArgs = []
//                    for (def a : confArgs) { baseArgs << substitute(String.valueOf(a), tokens) }

                    File outFile = inPlace ? current : new File(workDir, "${stageName}_step${i}" + (outExt ?: extensionFor(format, current)))
                    if (stdout) {
                        // stdout tools: only add input file if not already specified
                        if (!baseArgs.any { it.contains(current.absolutePath) } && !baseArgs.any { it.contains(TOKEN_IN) }) baseArgs << current.absolutePath
                        // Output comes from stdout, no output file argument needed
                    } else if (!inPlace) {
                        // Regular tools: add input and output files if not specified
                        if (!baseArgs.any { it.contains(current.absolutePath) } && !baseArgs.any { it.contains(TOKEN_IN) }) baseArgs << current.absolutePath
                        if (!baseArgs.any { it.contains(outFile.absolutePath) } && !baseArgs.any { it.contains(TOKEN_OUT) }) baseArgs << outFile.absolutePath
                    } else {
                        // inPlace tools: only add input file if not already specified
                        if (!baseArgs.any { it.contains(current.absolutePath) } && !baseArgs.any { it.contains(TOKEN_IN) }) baseArgs << current.absolutePath
                    }

                    long before = current.length()
                    File backup = null
                    if (inPlace) {
                        backup = new File(workDir, "${stageName}_backup_step${i}" + extensionFor(format, current))
                        Files.copy(current, backup)
                    }
                    long startNanos = System.nanoTime()
                    CommandExecutor.ExecResult execRes
                    String usedToolName = toolName
                    if (type == 'java') {
                        // Execute Java SPI tool
                        File outTarget = inPlace ? current : outFile
                        execRes = execJavaTool(resolved, current, outTarget, step, [format: format, stage: stageName, pipeline: pipelineName, workDir: workDir])
                    } else {
                        // Process-based tool
                        String cmd = resolved.cmd
                        usedToolName = cmd ?: toolName
                        // Capture stdout as bytes if tool writes output to stdout
                        execRes = commandExecutor.exec(cmd, baseArgs, workDir, null, timeoutSeconds, stdout ? outFile : null)
                    }
                    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)
                    getTimer('imageopt.tool.time', 'Time per tool', [tool: usedToolName, format: format, stage: stageName])?.record(Duration.ofMillis(elapsedMs))

                    if (execRes.exitCode != 0) {
                        String msg = "Tool '${usedToolName}' exited with ${execRes.exitCode}: ${truncate(execRes.stderr, maxLog)}"
                        log.warn(msg)
                        addWarning(result, msg)
                        if (inPlace && backup?.exists()) { Files.copy(backup, current); backup.delete() }
                        continue
                    }

                    if (!inPlace) {
                        // For regular and stdout tools, check output file was created
                        if (!outFile.exists() || (outFile.length() == 0)) {
                            String msg = "Tool '${usedToolName}' did not produce output file: ${outFile.name}"
                            log.warn(msg)
                            addWarning(result, msg)
                            continue
                        }
                        long after = outFile.length()
                        if (alwaysAccept || (after > 0 && (acceptLarger || after <= before))) {
                            // adopt
                            if (targetFormat && targetFormat != format) {
                                // switch working file to new extension and format
                                File newCurrent = new File(workDir, 'current' + extensionFor(targetFormat, current))
                                if (current.exists()) current.delete()
                                Files.copy(outFile, newCurrent)
                                current = newCurrent
                                format = targetFormat
                            } else {
                                Files.copy(outFile, current)
                            }
                            getCounter('imageopt.tool.success', 'Tools succeeded', [tool: usedToolName, format: format, stage: stageName])?.increment()
                            incrementCounter('imageopt.bytes_saved', 'Bytes saved by tool', [tool: usedToolName, format: format, stage: stageName], (before - after))
                        } else {
                            String msg = "Tool '${usedToolName}' output larger (before=${before}, after=${after}) — discarding"
                            log.debug(msg)
                            addWarning(result, msg)
                        }
                        outFile.delete()
                    } else {
                        long after = current.length()
                        if (alwaysAccept || acceptLarger || after <= before) {
                            getCounter('imageopt.tool.success', 'Tools succeeded', [tool: usedToolName, format: format, stage: stageName])?.increment()
                            incrementCounter('imageopt.bytes_saved', 'Bytes saved by tool', [tool: usedToolName, format: format, stage: stageName], (before - after))
                        } else {
                            String msg = "In-place tool '${usedToolName}' increased size (before=${before}, after=${after}) — rolling back"
                            log.debug(msg)
                            addWarning(result, msg)
                            if (backup?.exists()) {
                                Files.copy(backup, current); backup.delete() } else { Files.copy(inputFile, current)
                            }
                        }
                    }
                }

                long afterStage = current.length()
                getCounter('imageopt.stage.completed', 'Stages completed', [format: format, stage: stageName])?.increment()
                if (afterStage < beforeStage) {
                    incrementCounter('imageopt.stage.bytes_saved', 'Bytes saved per stage', [format: format, stage: stageName], (beforeStage - afterStage))
                }
            }

            result.optimisedFile = current
            result.optimisedBytes = current.length()
            result.outputContentType = mimeFor(format)
            long saved = result.originalBytes - result.optimisedBytes
            if (saved > 0) {
                incrementCounter('imageopt.total_saved', 'Total bytes saved', [format: format], saved)
            }
            return result
        }

        // No stages configured
        log.debug('No optimisation stages configured; leaving file unchanged')
        return result
    }

    /**
     * Resolve any tool (process or java) using fallback chain. For process tools, requires installed cmd.
     * For java tools, requires loadable class.
     */
    private Tool resolveAnyToolWithFallback(String toolName, Map<String, Tool> toolsCfg) {
        if (toolsCfg == null) return null
        Set<String> visited = new LinkedHashSet<>()
        LinkedList<String> queue = new LinkedList<>()
        queue.add(toolName)
        while (!queue.isEmpty()) {
            String name = queue.removeFirst()
            if (!visited.add(name)) continue
            Tool t = toolsCfg[name]
            if (t == null) continue
            String type = t.type
            if (type == 'java') {
                // Check class can be loaded
                String className = t.className
                if (className) {
                    try {
                        Class.forName(className)
                        return t
                    } catch (Throwable ignore) {
                        // try fallback
                    }
                }
            } else {
                String cmd = t.cmd
                if (cmd && commandExecutor.isInstalled(cmd)) {
                    return t
                }
            }
            List<String> fb = t.fallback
            if (fb) {
                for (String n : fb) if (n != null) queue.add(n)
            }
        }
        return null
    }

    private CommandExecutor.ExecResult execJavaTool(Tool toolCfg, File inputFile, File outputFile, StageStep stepCfg, Map ctx) {
        String className = toolCfg.className
        if (!className) {
            def r = new CommandExecutor.ExecResult(exitCode: 1, stdout: '', stderr: 'Java tool missing class name')
            return r
        }
        ImageOptimTool tool
        synchronized (javaTools) {
            tool = javaTools.get(className)
            if (tool == null) {
                try {
                    Class<?> clazz = Class.forName(className)
                    tool = (ImageOptimTool) clazz.getDeclaredConstructor().newInstance()
                    tool.init(toolConfigToMap(toolCfg))
                    javaTools.put(className, tool)
                } catch (Throwable e) {
                    def r = new CommandExecutor.ExecResult(exitCode: 1, stdout: '', stderr: 'Failed to load java tool ' + className + ': ' + e.message)
                    return r
                }
            }
        }
        try {
            return tool.run(inputFile, outputFile, stepConfigToMap(stepCfg), ctx ?: [:])
        } catch (Throwable e) {
            return new CommandExecutor.ExecResult(exitCode: 1, stdout: '', stderr: 'Java tool error: ' + e.message)
        }
    }

    private static Map toolConfigToMap(Tool tool) {
        [
            cmd: tool.cmd,
            type: tool.type,
            inPlace: tool.inPlace,
            fallback: tool.fallback,
            'class': tool.className
        ]
    }

    private static Map stepConfigToMap(StageStep step) {
        Map config = [
            tool: step.tool,
            lossy: step.lossy,
            outputFormat: step.outputFormat,
            acceptLarger: step.acceptLarger,
            alwaysAccept: step.alwaysAccept,
            args: step.args,
            maxWidth: step.maxWidth,
            maxHeight: step.maxHeight
        ]
        // Merge in any generic properties (e.g., useScalr, quality, etc.)
        if (step.properties) {
            config.putAll(step.properties)
        }
        return config
    }

    private static String substitute(String arg, Map<String, String> tokens) {
        String s = arg
        tokens.each { k, v ->
            s = s.replace(k, v)
        }
        return s
    }

    private static Map<String, String> buildTokens(File current, File workDir, String format, Map profile, String outExt = null) {
        int q = (profile.quality instanceof Number) ? ((Number) profile.quality).intValue() : 82
        String qRange = profile.qualityRange ?: '65-80'
        [
                (TOKEN_IN): current.absolutePath,
                (TOKEN_OUT): new File(workDir, 'out' + (outExt ?: extensionFor(format, current))).absolutePath,
                (TOKEN_QUALITY): String.valueOf(q),
                (TOKEN_QUALITY_RANGE): String.valueOf(qRange)
        ]
    }

    private static boolean passesFilter(File current, String contentType, String format, Filter filter) {
        if (!filter) return true
        long minBytes = filter.minBytes
        long minBytesByFmt = 0L
        Map<String, Long> byFmt = filter.minBytesByFormat
        if (byFmt && format) {
            Long v = byFmt[format]
            if (v != null) {
                minBytesByFmt = v
            }
        }
        long effectiveMinBytes = (minBytesByFmt > 0L) ? minBytesByFmt : minBytes
        long minPixels = filter.minPixels
        long minWidth = filter.minWidth
        long minHeight = filter.minHeight
        List<String> types = filter.contentTypes
        if (effectiveMinBytes > 0 && current.length() < effectiveMinBytes) return false
        if (minPixels > 0 || minWidth > 0 || minHeight > 0) {
            try {
                def dims = ImageReaderUtils.getImageDimensions(Files.asByteSource(current), current.name)
                long px = dims ? (dims.width as long) * (dims.height as long) : 0L
                if (minPixels > 0 && px < minPixels) return false
                if (minWidth > 0 && (dims?.width ?: 0) < minWidth) return false
                if (minHeight > 0 && (dims?.height ?: 0) < minHeight) return false
            } catch (Throwable ignore) {
                return false
            }
        }
        if (types && contentType) {
            boolean match = types.any { contentType.toLowerCase().startsWith(String.valueOf(it).toLowerCase()) }
            if (!match) return false
        }
        // animated and alpha filters (optional): if specified, must match
        if (filter.animated != null) {
            boolean wantAnimated = filter.animated
            boolean isAnim = false
            try { isAnim = isAnimatedGif(current) } catch (Throwable ignore) { isAnim = false }
            if (wantAnimated != isAnim) return false
        }
        if (filter.alpha != null) {
            boolean wantAlpha = filter.alpha
            boolean hasAlpha = false
            try { hasAlpha = hasAlphaPng(current) } catch (Throwable ignore) { hasAlpha = false }
            if (wantAlpha != hasAlpha) return false
        }
        return true
    }

    private static String extensionFor(String format, File file) {
        switch (format) {
            case 'jpeg': return '.jpg'
            case 'png': return '.png'
            case 'gif': return '.gif'
            case 'webp': return '.webp'
            case 'avif': return '.avif'
            case 'svg': return '.svg'
            default:
                String name = file.name
                int i = name.lastIndexOf('.')
                return i > -1 ? name.substring(i) : ''
        }
    }

    private static String inferFormat(String contentType, File file) {
        // Prefer OS probe
        String ct = null
        try { ct = probeContentType(file.toPath()) } catch (Throwable ignore) {}
        if (!ct && contentType) ct = contentType
        if (ct) {
            ct = ct.toLowerCase()
            if (ct.startsWith('image/jpeg') || ct == 'image/jpg') return 'jpeg'
            if (ct.startsWith('image/png')) return 'png'
            if (ct.startsWith('image/gif')) return 'gif'
            if (ct.startsWith('image/webp')) return 'webp'
            if (ct.startsWith('image/avif')) return 'avif'
            if (ct.startsWith('image/svg')) return 'svg'
            // Fallback to Tika to see if we can normalize
            def ext = inferExtensionFromContentType(ct)
            if (ext == 'jpg') return 'jpeg'
            if (['jpeg','png','gif','webp','avif','svg'].contains(ext)) return ext
        }
        // else fallback to filename; if unknown, default jpeg
        String name = file.name.toLowerCase()
        return inferFormatForFilename(name) ?: 'jpeg'
    }

    private static String inferExtensionFromContentType(String ct) {
        try {
            if (ct == 'application/octet-stream') {
                log.warn('Got $ct')
                // todo return ''?
            }
            MimeTypes allTypes = MimeTypes.getDefaultMimeTypes()
            MimeType mimeType = allTypes.forName(ct)
            String ext = mimeType.getExtension() // like ".jpg"
            if (ext?.startsWith('.')) ext = ext.substring(1)
            return ext
        } catch (Throwable ignore) {
            return ''
        }
    }

    static String inferFormatForFilename(String name) {
        if (name.endsWith('.jpg') || name.endsWith('.jpeg')) return 'jpeg'
        if (name.endsWith('.png')) return 'png'
        if (name.endsWith('.gif')) return 'gif'
        if (name.endsWith('.webp')) return 'webp'
        if (name.endsWith('.avif')) return 'avif'
        if (name.endsWith('.svg')) return 'svg'
        return null
    }

    private static String mimeFor(String format) {
        switch (String.valueOf(format)) {
            case 'jpeg': return 'image/jpeg'
            case 'png': return 'image/png'
            case 'gif': return 'image/gif'
            case 'webp': return 'image/webp'
            case 'avif': return 'image/avif'
            case 'svg': return 'image/svg+xml'
            default: return 'application/octet-stream'
        }
    }

    static String inferExtensionForUrlOrContentType(String url, String contentType) {
        // use content Type first
        def ctExt = inferExtensionFromContentType(contentType)
        if (!ctExt || ctExt == 'bin') {
            String ext = url ? FilenameUtils.getExtension(ImageUtils.getFilename(url)) : ''
            return ext ?: ctExt
        }
        return ctExt
    }

    private static String truncate(String s, int max) {
        if (s == null) return ''
        if (max <= 0) return ''
        if (s.length() <= max) return s
        return s.substring(0, Math.max(0, max)) + '…(truncated)'
    }

    private static final int MAX_WARNING_LIST = 50

    private static void addWarning(OptimisationResult res, String msg) {
        if (res == null || msg == null) return
        // Aggregate counts
        Integer c = res.warningsSummary.get(msg)
        res.warningsSummary.put(msg, (c == null ? 1 : (c + 1)))
        // Keep a capped list of unique recent warnings
        if (res.warnings.size() < MAX_WARNING_LIST) {
            res.warnings << msg
        }
    }

    // Feature detection helpers
    protected static boolean isAnimatedGif(File file) {
        try {
            // Quick check by extension/content type inference is already done; we can count images
            final def bytes = Files.asByteSource(file)
            return ImageReaderUtils.withImageReader(bytes, false, false, { ImageReader reader ->
                try {
                    return (reader.getNumImages(true) > 1)
                } catch (Throwable ignore) {
                    return false
                }
            } as Function<ImageReader, Boolean>)
        } catch (Throwable ignore) {
            return false
        }
    }

    protected static boolean hasAlphaPng(File file) {
        try {
            final def bytes = Files.asByteSource(file)
            return ImageReaderUtils.withImageReader(bytes, false, false, { ImageReader reader ->
                try {
                    def it = reader.getImageTypes(0)
                    if (it == null) return false
                    while (it.hasNext()) {
                        def type = it.next()
                        def cm = type.getColorModel()
                        if (cm != null && cm.hasAlpha()) return true
                    }
                    return false
                } catch (Throwable ignore) {
                    return false
                }
            } as Function<ImageReader,Boolean>)
        } catch (Throwable ignore) {
            return false
        }
    }

}
