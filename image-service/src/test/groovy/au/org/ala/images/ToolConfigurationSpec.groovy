package au.org.ala.images

import au.org.ala.images.config.*
import au.org.ala.images.optimisation.CommandExecutor
import spock.lang.Specification
import spock.lang.TempDir

class ToolConfigurationSpec extends Specification {

    @TempDir
    File tempDir

    static class CapturingExec implements CommandExecutor {
        List<Map> calls = []
        Set<String> installedTools = []

        @Override
        boolean isInstalled(String cmd) {
            return installedTools.isEmpty() || installedTools.contains(cmd)
        }

        @Override
        ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds) {
            calls << [cmd: cmd, args: args, stdout: false]
            return new ExecResult(exitCode: 0, stdout: '', stderr: '')
        }

        @Override
        ExecResult exec(String cmd, List<String> args, File workingDir, Map<String, String> env, long timeoutSeconds, File stdoutFile) {
            calls << [cmd: cmd, args: args, stdout: (stdoutFile != null)]
            if (stdoutFile) {
                stdoutFile.parentFile?.mkdirs()
                stdoutFile.text = "dummy output"
            }
            return new ExecResult(exitCode: 0, stdout: '', stderr: '')
        }

        @Override
        ExecResult exec(String cmd, List<String> args, File workingDir, InputStream stdinStream, long timeoutSeconds, OutputStream stdoutStream) {
            calls << [cmd: cmd, args: args, stdout: true]
            stdoutStream.write("dummy output".getBytes())
            return new ExecResult(exitCode: 0, stdout: '', stderr: '')
        }
    }

    def "verify all default tool configurations"() {
        given:
        def service = new ImageOptimisationService()
        def capturer = new CapturingExec()
        service.commandExecutor = capturer

        def config = new ImageOptimisationConfig()
        config.skipThresholdBytes = -1
        
        // Remove filters from stages to ensure they run
        config.stages.each { name, stages ->
            stages.each { it.filter = null }
        }

        service.imageOptimisationConfig = config

        when: "Running all toolsets for their intended formats"
        config.toolsets.each { toolsetName, toolsetMap ->
            toolsetMap.each { format, steps ->
                // Normalise 'all' to something we can test, like 'jpeg' or 'png'
                String testFormat = format == ImageOptimisationConfig.ALL_FORMATS ? 'image/jpeg' : "image/${format}"
                if (format == 'svg') testFormat = 'image/svg+xml'

                // Create a fresh file for each run to avoid side effects
                File f = new File(tempDir, "test_${toolsetName}_${format}.${format == 'all' ? 'jpg' : format}")
                f.text = "some content for ${toolsetName} ${format} " * 100

                capturer.calls.clear()
                // Use the stage directly if we can't get pipeline to work
                def stage = new Stage(name: 'test', toolsRef: toolsetName, allowLossy: true)
                config.stages['testPipe'] = [stage]

                service.optimise(f, testFormat, 'testPipe')
            }
        }

        then: "No exceptions thrown and basic argument sanity"
        noExceptionThrown()
    }

    def "aggressive JPEG optimisation prefers vips and falls back to convert with JPEG input and output"() {
        given:
        def service = new ImageOptimisationService()
        def capturer = new CapturingExec()
        service.commandExecutor = capturer

        def config = new ImageOptimisationConfig()
        config.skipThresholdBytes = -1
        Tool vipsJpeg = config.tools['vipsJpeg']
        assert vipsJpeg.cmd == 'vips'
        assert vipsJpeg.fallback == ['convertJpeg']
        assert !config.tools.containsKey('mozjpeg')

        // Force the pipeline to include our stage
        config.stages['testAggressive'] = [new Stage(name: 'opt', toolsRef: 'optimAggressive', allowLossy: true)]

        service.imageOptimisationConfig = config

        File f = new File(tempDir, "test.jpg")
        f.text = "test content " * 100

        when: 'libvips is available'
        capturer.installedTools = ['vips'] as Set
        service.optimise(f, 'image/jpeg', 'testAggressive')

        then:
        def vipsCall = capturer.calls.find { it.cmd == 'vips' }
        vipsCall.args[0] == 'copy'
        vipsCall.args[1].endsWith('current.jpg')
        vipsCall.args[2].endsWith('opt_step0.jpg[Q=75,interlace,strip,optimize_coding]')

        when: 'libvips is unavailable'
        capturer.calls.clear()
        capturer.installedTools = ['convert'] as Set
        service.optimise(f, 'image/jpeg', 'testAggressive')

        then:
        def convertCall = capturer.calls.find { it.cmd == 'convert' }
        convertCall.args[0].endsWith('current.jpg')
        convertCall.args.containsAll(['-strip', '-interlace', 'Plane', '-quality', '75'])
        convertCall.args.last().endsWith('opt_step0.jpg')
    }

    def "verify pngquant configuration handles output argument correctly"() {
        given:
        def service = new ImageOptimisationService()
        def capturer = new CapturingExec()
        service.commandExecutor = capturer

        def config = new ImageOptimisationConfig()
        config.skipThresholdBytes = -1
        Tool pngquant = config.tools['pngquant']
        assert pngquant != null
        assert pngquant.stdout == false
        assert pngquant.inPlace == false

        config.stages['testPng'] = [new Stage(name: 'opt', toolsRef: 'optimAggressive', allowLossy: true)]

        service.imageOptimisationConfig = config

        File f = new File(tempDir, "test.png")
        f.text = "test content " * 100

        when:
        // Use a pipeline that definitely contains pngquant
        service.optimise(f, 'image/png', 'testPng')

        then:
        def call = capturer.calls.find { it.cmd == 'pngquant' }
        call != null
        call.stdout == false
        // pngquant in optimAggressive uses --output=%OUT%
        call.args.any { it.startsWith('--output=') && it.contains("step") }
        // Should NOT have an extra positional OUT argument because %OUT% was in args
        def outArgs = call.args.findAll { it.contains("step") }
        outArgs.size() == 1
    }
}
