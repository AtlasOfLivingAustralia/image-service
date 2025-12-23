package au.org.ala.images.config

import grails.config.Config
import grails.core.GrailsApplication
import grails.testing.spring.AutowiredTest
import spock.lang.Specification

class ImageOptimisationConfigLoaderSpec extends Specification implements AutowiredTest {

    Closure doWithSpring() {{ ->
        imageOptimisationConfig(ImageOptimisationConfig)
        imageOptimisationConfigLoader(ImageOptimisationConfigLoader)
    }}

    void "test config loader loads stages from config"() {
        given:
        GrailsApplication grailsApplication = Mock(GrailsApplication)
        Config config = Mock(Config)
        ImageOptimisationConfig imageOptimisationConfig = new ImageOptimisationConfig()
        ImageOptimisationConfigLoader loader = new ImageOptimisationConfigLoader()
        loader.grailsApplication = grailsApplication
        loader.imageOptimisationConfig = imageOptimisationConfig

        // Mock the config structure
        def stagesConfig = [
                default: [
                        [
                                name: 'test-stage',
                                filter: [
                                        minBytes: 100000
                                ],
                                toolsRef: 'testTools',
                                allowLossy: true
                        ]
                ]
        ]

        grailsApplication.config >> config
        config.getProperty('images.optimisation.enabled', Boolean, true) >> true
        config.getProperty('images.optimisation.tempDir', String, _) >> '/tmp'
        config.getProperty('images.optimisation.timeoutSecondsPerTool', Integer, 30) >> 30
        config.getProperty('images.optimisation.skipThresholdBytes', Long, 50_000L) >> 50_000L
        config.getProperty('images.optimisation.maxLogChars', Integer, 2000) >> 2000
        config.getProperty('images.optimisation.stages') >> stagesConfig
        config.getProperty('images.optimisation.toolsets') >> null
        config.getProperty('images.optimisation.tools') >> null

        when:
        loader.loadConfig()

        then:
        imageOptimisationConfig.stages != null
        imageOptimisationConfig.stages['default'] != null
        imageOptimisationConfig.stages['default'].size() == 1
        imageOptimisationConfig.stages['default'][0].name == 'test-stage'
        imageOptimisationConfig.stages['default'][0].toolsRef == 'testTools'
        imageOptimisationConfig.stages['default'][0].allowLossy == true
        imageOptimisationConfig.stages['default'][0].filter != null
        imageOptimisationConfig.stages['default'][0].filter.minBytes == 100000
    }

    void "test config loader loads toolsets from config"() {
        given:
        GrailsApplication grailsApplication = Mock(GrailsApplication)
        Config config = Mock(Config)
        ImageOptimisationConfig imageOptimisationConfig = new ImageOptimisationConfig()
        ImageOptimisationConfigLoader loader = new ImageOptimisationConfigLoader()
        loader.grailsApplication = grailsApplication
        loader.imageOptimisationConfig = imageOptimisationConfig

        def toolsetsConfig = [
                testToolset: [
                        jpeg: [
                                [
                                        tool: 'jpegtran',
                                        args: ['-optimize'],
                                        lossy: false
                                ]
                        ]
                ]
        ]

        grailsApplication.config >> config
        config.getProperty('images.optimisation.enabled', Boolean, true) >> true
        config.getProperty('images.optimisation.tempDir', String, _) >> '/tmp'
        config.getProperty('images.optimisation.timeoutSecondsPerTool', Integer, 30) >> 30
        config.getProperty('images.optimisation.skipThresholdBytes', Long, 50_000L) >> 50_000L
        config.getProperty('images.optimisation.maxLogChars', Integer, 2000) >> 2000
        config.getProperty('images.optimisation.stages') >> null
        config.getProperty('images.optimisation.toolsets') >> toolsetsConfig
        config.getProperty('images.optimisation.tools') >> null

        when:
        loader.loadConfig()

        then:
        imageOptimisationConfig.toolsets != null
        imageOptimisationConfig.toolsets['testToolset'] != null
        imageOptimisationConfig.toolsets['testToolset']['jpeg'] != null
        imageOptimisationConfig.toolsets['testToolset']['jpeg'].size() == 1
        imageOptimisationConfig.toolsets['testToolset']['jpeg'][0].tool == 'jpegtran'
        imageOptimisationConfig.toolsets['testToolset']['jpeg'][0].args == ['-optimize']
        imageOptimisationConfig.toolsets['testToolset']['jpeg'][0].lossy == false
    }

    void "test config loader loads tools from config"() {
        given:
        GrailsApplication grailsApplication = Mock(GrailsApplication)
        Config config = Mock(Config)
        ImageOptimisationConfig imageOptimisationConfig = new ImageOptimisationConfig()
        ImageOptimisationConfigLoader loader = new ImageOptimisationConfigLoader()
        loader.grailsApplication = grailsApplication
        loader.imageOptimisationConfig = imageOptimisationConfig

        def toolsConfig = [
                jpegtran: [
                        cmd: 'jpegtran',
                        type: 'process',
                        stdout: true,
                        inPlace: false
                ],
                javaTool: [
                        type: 'java',
                        className: 'com.example.MyTool'
                ]
        ]

        grailsApplication.config >> config
        config.getProperty('images.optimisation.enabled', Boolean, true) >> true
        config.getProperty('images.optimisation.tempDir', String, _) >> '/tmp'
        config.getProperty('images.optimisation.timeoutSecondsPerTool', Integer, 30) >> 30
        config.getProperty('images.optimisation.skipThresholdBytes', Long, 50_000L) >> 50_000L
        config.getProperty('images.optimisation.maxLogChars', Integer, 2000) >> 2000
        config.getProperty('images.optimisation.stages') >> null
        config.getProperty('images.optimisation.toolsets') >> null
        config.getProperty('images.optimisation.tools') >> toolsConfig

        when:
        loader.loadConfig()

        then:
        imageOptimisationConfig.tools != null
        imageOptimisationConfig.tools['jpegtran'] != null
        imageOptimisationConfig.tools['jpegtran'].cmd == 'jpegtran'
        imageOptimisationConfig.tools['jpegtran'].stdout == true
        imageOptimisationConfig.tools['jpegtran'].inPlace == false
        imageOptimisationConfig.tools['javaTool'] != null
        imageOptimisationConfig.tools['javaTool'].type == 'java'
        imageOptimisationConfig.tools['javaTool'].className == 'com.example.MyTool'
    }

    void "test config loader preserves defaults when no config present"() {
        given:
        GrailsApplication grailsApplication = Mock(GrailsApplication)
        Config config = Mock(Config)
        ImageOptimisationConfig imageOptimisationConfig = new ImageOptimisationConfig()
        ImageOptimisationConfigLoader loader = new ImageOptimisationConfigLoader()
        loader.grailsApplication = grailsApplication
        loader.imageOptimisationConfig = imageOptimisationConfig

        // Store original default values
        def originalStages = imageOptimisationConfig.stages
        def originalToolsets = imageOptimisationConfig.toolsets
        def originalTools = imageOptimisationConfig.tools

        grailsApplication.config >> config
        config.getProperty('images.optimisation.enabled', Boolean, true) >> true
        config.getProperty('images.optimisation.tempDir', String, _) >> '/tmp'
        config.getProperty('images.optimisation.timeoutSecondsPerTool', Integer, 30) >> 30
        config.getProperty('images.optimisation.skipThresholdBytes', Long, 50_000L) >> 50_000L
        config.getProperty('images.optimisation.maxLogChars', Integer, 2000) >> 2000
        config.getProperty('images.optimisation.stages') >> null
        config.getProperty('images.optimisation.toolsets') >> null
        config.getProperty('images.optimisation.tools') >> null

        when:
        loader.loadConfig()

        then:
        // Defaults should be preserved
        imageOptimisationConfig.stages == originalStages
        imageOptimisationConfig.toolsets == originalToolsets
        imageOptimisationConfig.tools == originalTools
    }
}

