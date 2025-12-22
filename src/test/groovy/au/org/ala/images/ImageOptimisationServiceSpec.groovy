package au.org.ala.images

import au.org.ala.images.config.*
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ImageResizeTool
import com.google.common.io.Files
import spock.lang.Specification
import spock.lang.TempDir

import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import au.org.ala.images.util.ImageReaderUtils

class ImageOptimisationServiceSpec extends Specification {

    @TempDir
    File tempDir

    static class FakeExec implements CommandExecutor {
        @Override
        boolean isInstalled(String cmd) {
            return cmd.startsWith('fake-')
        }

        @Override
        ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds) {
            return exec(cmd, args, workingDir, env, timeoutSeconds, null)
        }

        @Override
        ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile) {
            def res = new ExecResult(exitCode: 0, stdout: '', stderr: '')
            if (cmd == 'fake-shrink') {
                // last arg is OUT path (service ensures IN/OUT when not inPlace)
                File out = new File(args.last())
                out.parentFile?.mkdirs()
                // write smaller file
                out.text = '1234567890' // 10 bytes
            } else if (cmd == 'fake-grow') {
                File out = new File(args.last())
                out.parentFile?.mkdirs()
                out.text = 'x' * (new File(args.first()).length() + 1) // one extra byte
            } else if (cmd == 'fake-fail') {
                res.exitCode = 1
                res.stderr = 'simulated failure'
            }
            return res
        }
    }

    def 'inferFormat normalises image/jpg to jpeg key so per-format toolset runs'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        // Use a file without image extension so filename fallback doesn't interfere
        File f = new File(tempDir, 'noext'); f.text = 'x' * 2000 // 2KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'fmt'
        stage.filter = Filter.minBytes(0).minBytesByFormat([jpeg: 1500L])
        stage.toolsRef = 'shrinkTs'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            shrinkTs: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('shrink')]]
        ]

        config.tools = [
            shrink: Tool.process('fake-shrink', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpg')

        then:
        res.optimisedBytes < res.originalBytes
    }

    def 'acceptLarger allows adopting larger non in-place output'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f = new File(tempDir, 'a.jpg'); f.text = 'x' * 200 // 200 bytes

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'grow'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'pipe'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            pipe: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('grow').acceptLarger(true)]]
        ]

        config.tools = [
            grow: Tool.process('fake-grow', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.optimisedBytes > res.originalBytes
    }

    def 'default behaviour discards larger non in-place output'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f = new File(tempDir, 'a.jpg'); f.text = 'x' * 2000 // 2KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'grow'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'pipe'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            pipe: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('grow')]]
        ]

        config.tools = [
            grow: Tool.process('fake-grow', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.optimisedBytes == res.originalBytes
    }

    def 'alwaysAccept adopts output regardless of size for in-place and non in-place'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f1 = new File(tempDir, 'a.jpg'); f1.text = 'x' * 1000 // 1KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'grow'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'pipe'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            pipe: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('grow').alwaysAccept(true)]]
        ]

        config.tools = [
            grow: Tool.process('fake-grow', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res1 = service.optimise(f1, 'image/jpeg')

        then:
        res1.optimisedBytes > res1.originalBytes
    }

    def 'warnings aggregated and stderr truncated'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec() {
            @Override
            CommandExecutor.ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile) {
                def res = new CommandExecutor.ExecResult()
                res.exitCode = 1
                // Long stderr to trigger truncation
                res.stderr = 'E' * 5000
                res.stdout = ''
                return res
            }
            @Override
            boolean isInstalled(String cmd) { return true }
        }
        File f = new File(tempDir, 'a.jpg'); f.text = 'x' * 2000 // 2KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0
        config.maxLogChars = 1000

        Stage stage = new Stage()
        stage.name = 'failtwice'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'pipe'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            pipe: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('fail'), StageStep.of('fail')]]
        ]

        config.tools = [
            fail: Tool.process('fake-fail', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.warnings.size() >= 1
        // Aggregated count for identical messages should be 2
        res.warningsSummary.values().any { it >= 2 }
        // Truncation marker present in first warning
        res.warnings.first().contains('truncated')
    }

    def 'cross-format conversion adopts new format and updates MIME type'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec() {
            @Override
            boolean isInstalled(String cmd) { return true }

            @Override
            CommandExecutor.ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdinFile) {
                // Simulate a converter that writes to %OUT%
                def res = new CommandExecutor.ExecResult(exitCode: 0, stdout: '', stderr: '')
                File out = new File(args.last())
                out.parentFile?.mkdirs()
                out.text = 'converted-to-jpeg' // bytes smaller than input
                return res
            }
        }
        // Start with a PNG file larger than output
        File f = new File(tempDir, 'src.png'); f.text = 'x' * 2000 // 2KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'to-jpeg'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'jpegConvert'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            jpegConvert: [(ImageOptimisationConfig.ALL_FORMATS): [
                StageStep.lossy('convert', ['%IN%', '%OUT%']).outputFormat('jpeg')
            ]]
        ]

        config.tools = [
            convert: Tool.process('fake-convert', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/png')

        then:
        !res.skipped
        res.outputContentType == 'image/jpeg'
        res.optimisedFile.name.endsWith('.jpg')
        res.optimisedBytes < res.originalBytes
    }

    def 'java tool resize downsizes image when exceeding maxWidth/maxHeight'() {
        given:
        // Create a 800x600 PNG
        BufferedImage img = new BufferedImage(800, 600, BufferedImage.TYPE_INT_RGB)
        File src = new File(tempDir, 'src.png')
        ImageIO.write(img, 'png', src)

        def service = new ImageOptimisationService()
        // Command executor won't be used for java tools, but provide a permissive fake for safety
        service.commandExecutor = new FakeExec() { @Override boolean isInstalled(String cmd) { return true } }

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'resize'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'resizeScalr'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            resizeScalr: [(ImageOptimisationConfig.ALL_FORMATS): [
                StageStep.lossy('javaResize', []).maxSize(400, 400)
            ]]
        ]

        config.tools = [
            javaResize: Tool.java(ImageResizeTool)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(src, 'image/png')

        then:
        !res.skipped
        res.optimisedFile.exists()
        def dims = ImageReaderUtils.getImageDimensions(Files.asByteSource(res.optimisedFile), res.optimisedFile.name)
        dims.width <= 400
        dims.height <= 400
    }

    def 'minWidth and minHeight filters control stage execution'() {
        given:
        // Create two images: 100x100 and 300x100
        BufferedImage a = new BufferedImage(100, 100, BufferedImage.TYPE_INT_RGB)
        File small = new File(tempDir, 'small.png'); ImageIO.write(a, 'png', small)
        BufferedImage b = new BufferedImage(300, 100, BufferedImage.TYPE_INT_RGB)
        File wide = new File(tempDir, 'wide.png'); ImageIO.write(b, 'png', wide)

        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec() { @Override boolean isInstalled(String cmd) { return true } }

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'gate'
        stage.filter = Filter.minBytes(0).minWidth(200).minHeight(50)
        stage.toolsRef = 'pipe'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            pipe: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('grow').alwaysAccept(true)]]
        ]

        config.tools = [
            grow: Tool.process('fake-grow', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def resSmall = service.optimise(small, 'image/png')
        def resWide = service.optimise(wide, 'image/png')

        then:
        // small should be skipped (no tool runs), wide should run and grow
        resSmall.optimisedBytes == resSmall.originalBytes
        resWide.optimisedBytes > resWide.originalBytes
    }

    def 'staged filter: runs when minBytesByFormat for jpeg is met'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f = new File(tempDir, 'b.jpg'); f.text = 'x' * 2000 // 2KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'fmt-threshold'
        stage.filter = Filter.minBytes(0).minBytesByFormat([jpeg: 1500L])
        stage.toolsRef = 'shrinkTs'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            shrinkTs: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('shrink')]]
        ]

        config.tools = [
            shrink: Tool.process('fake-shrink', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        !res.skipped
        res.optimisedBytes < res.originalBytes
    }

    def 'staged filter: skips when below minBytesByFormat threshold'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f = new File(tempDir, 'c.jpg'); f.text = 'x' * 1000 // 1KB

        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        Stage stage = new Stage()
        stage.name = 'fmt-threshold'
        stage.filter = Filter.minBytes(0).minBytesByFormat([jpeg: 1500L])
        stage.toolsRef = 'shrinkTs'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            shrinkTs: [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('shrink')]]
        ]

        config.tools = [
            shrink: Tool.process('fake-shrink', false)
        ]

        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.optimisedBytes == res.originalBytes
    }

    private static ImageOptimisationConfig baseConfig(File tempDir) {
        def config = new ImageOptimisationConfig()
        config.enabled = true
        config.tempDir = tempDir.absolutePath
        config.timeoutSecondsPerTool = 2
        config.skipThresholdBytes = 0

        // Default staged pipeline; individual tests may override stages/toolsets
        Stage stage = new Stage()
        stage.name = 'default'
        stage.filter = Filter.minBytes(1)
        stage.toolsRef = 'pipe'
        stage.allowLossy = true
        config.stages = [default: [stage]]

        config.toolsets = [
            pipe: [(ImageOptimisationConfig.ALL_FORMATS): []] // tests will override with concrete steps
        ]

        config.tools = [
            shrink: Tool.process('fake-shrink', false),
            grow: Tool.process('fake-grow', false),
            fail: Tool.process('fake-fail', false)
        ]

        return config
    }

    def 'skips optimisation when below threshold'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f = new File(tempDir, 'a.jpg'); f.text = 'x' * 100 // 100 bytes
        def config = baseConfig(tempDir)
        config.skipThresholdBytes = 1000 // larger than file
        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.skipped
        res.optimisedFile.length() == f.length()
    }

    def 'handles missing tool gracefully'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec() {
            @Override
            boolean isInstalled(String cmd) { return false }
        }
        File f = new File(tempDir, 'a.jpg'); f.text = 'x' * 2000
        def config = baseConfig(tempDir)
        // Configure a stage that references a tool not present in config.tools
        config.toolsets.pipe = [(ImageOptimisationConfig.ALL_FORMATS): [StageStep.of('missing')]]
        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.warnings.size() >= 1
        res.optimisedBytes == res.originalBytes
    }

    def 'adopts smaller output and discards larger output'() {
        given:
        def service = new ImageOptimisationService()
        service.commandExecutor = new FakeExec()
        File f = new File(tempDir, 'a.jpg'); f.text = 'x' * 2000 // 2KB
        def config = baseConfig(tempDir)
        config.toolsets.pipe = [(ImageOptimisationConfig.ALL_FORMATS): [
            StageStep.of('shrink'),
            StageStep.of('grow')
        ]]
        service.imageOptimisationConfig = config

        when:
        def res = service.optimise(f, 'image/jpeg')

        then:
        res.optimisedBytes <= res.originalBytes
        res.optimisedFile.exists()
    }
}
