package au.org.ala.images.config

import grails.core.GrailsApplication
import groovy.transform.CompileStatic
import groovy.transform.CompileDynamic
import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component

import javax.annotation.PostConstruct

/**
 * Loads ImageOptimisationConfig from grailsApplication.config manually
 * because Spring Boot's @ConfigurationProperties doesn't properly deserialize
 * nested custom types in Grails (they remain as NavigableMap).
 *
 * Grabs the ImageOptimisationConfig bean and overwrites its properties
 * using the values read from the grails configuration but with the proper types.
 *
 * It does this in a @PostConstruct method and modifies the existing bean instance,
 * so that other beans that have already had ImageOptimisationConfig injected
 * will see the updated values.
 */
@Slf4j
@CompileStatic
@Component
class ImageOptimisationConfigLoader {

    @Autowired
    GrailsApplication grailsApplication

    @Autowired
    ImageOptimisationConfig imageOptimisationConfig

    @PostConstruct
    void loadConfig() {
        log.info("Loading ImageOptimisationConfig from grailsApplication.config")

        String prefix = 'images.optimisation'

        // Load simple properties
        imageOptimisationConfig.enabled = getConfigBoolean("${prefix}.enabled", true)
        imageOptimisationConfig.tempDir = getConfigString("${prefix}.tempDir", System.getProperty('java.io.tmpdir'))
        imageOptimisationConfig.timeoutSecondsPerTool = getConfigInt("${prefix}.timeoutSecondsPerTool", 30)
        imageOptimisationConfig.skipThresholdBytes = getConfigLong("${prefix}.skipThresholdBytes", 50_000L)
        imageOptimisationConfig.maxLogChars = getConfigInt("${prefix}.maxLogChars", 2000)

        // Load complex nested structures
        loadStages(prefix)
        loadToolsets(prefix)
        loadTools(prefix)

        log.info("ImageOptimisationConfig loaded: enabled={}, stages={}, toolsets={}, tools={}",
                imageOptimisationConfig.enabled,
                imageOptimisationConfig.stages?.size(),
                imageOptimisationConfig.toolsets?.size(),
                imageOptimisationConfig.tools?.size())
    }

    @CompileDynamic
    private void loadStages(String prefix) {
        def stagesConfig = getConfigMap("${prefix}.stages")
        if (!stagesConfig) {
            log.debug("No stages config found, using defaults")
            return
        }

        Map<String, List<Stage>> stages = [:]

        if (stagesConfig instanceof Map) {
            stagesConfig.each { String pipelineName, Object stagesList ->
                if (stagesList instanceof List) {
                    List<Stage> stageObjs = []
                    stagesList.each { Object stageData ->
                        Stage stage = parseStage(stageData)
                        if (stage) {
                            stageObjs.add(stage)
                        }
                    }
                    if (stageObjs) {
                        stages[pipelineName] = stageObjs
                    }
                }
            }
        }

        if (stages) {
            imageOptimisationConfig.stages = stages
        }
    }

    private Stage parseStage(Object data) {
        if (!(data instanceof Map)) {
            return null
        }

        Map map = (Map) data
        Stage stage = new Stage()

        stage.name = map.get('name')?.toString()
        stage.toolsRef = map.get('toolsRef')?.toString()
        stage.allowLossy = map.containsKey('allowLossy') ? Boolean.valueOf(map.get('allowLossy').toString()) : true

        // Parse filter
        if (map.containsKey('filter')) {
            stage.filter = parseFilter(map.get('filter'))
        }

        // Parse steps
        if (map.containsKey('steps')) {
            def stepsData = map.get('steps')
            if (stepsData instanceof List) {
                stage.steps = []
                stepsData.each { Object stepData ->
                    StageStep step = parseStageStep(stepData)
                    if (step) {
                        stage.steps.add(step)
                    }
                }
            }
        }

        return stage
    }

    private Filter parseFilter(Object data) {
        if (!(data instanceof Map)) {
            return null
        }

        Map map = (Map) data
        Filter filter = new Filter()

        if (map.containsKey('minBytes')) {
            filter.minBytes = parseLong(map.get('minBytes'))
        }
        if (map.containsKey('minPixels')) {
            filter.minPixels = parseLong(map.get('minPixels'))
        }
        if (map.containsKey('minWidth')) {
            filter.minWidth = parseLong(map.get('minWidth'))
        }
        if (map.containsKey('minHeight')) {
            filter.minHeight = parseLong(map.get('minHeight'))
        }
        if (map.containsKey('contentTypes')) {
            def ct = map.get('contentTypes')
            if (ct instanceof List) {
                filter.contentTypes = ct.collect { it.toString() }
            }
        }
        if (map.containsKey('animated')) {
            filter.animated = Boolean.valueOf(map.get('animated').toString())
        }
        if (map.containsKey('alpha')) {
            filter.alpha = Boolean.valueOf(map.get('alpha').toString())
        }
        if (map.containsKey('minBytesByFormat') && map.get('minBytesByFormat') instanceof Map) {
            Map<String, Long> minBytesByFormat = [:]
            ((Map) map.get('minBytesByFormat')).each { k, v ->
                minBytesByFormat[k.toString()] = parseLong(v)
            }
            filter.minBytesByFormat = minBytesByFormat
        }

        return filter
    }

    private StageStep parseStageStep(Object data) {
        if (!(data instanceof Map)) {
            return null
        }

        Map map = (Map) data
        StageStep step = new StageStep()

        step.tool = map.get('tool')?.toString()
        step.lossy = map.containsKey('lossy') ? Boolean.valueOf(map.get('lossy').toString()) : false
        step.outputFormat = map.get('outputFormat')?.toString()
        step.acceptLarger = map.containsKey('acceptLarger') ? Boolean.valueOf(map.get('acceptLarger').toString()) : false
        step.alwaysAccept = map.containsKey('alwaysAccept') ? Boolean.valueOf(map.get('alwaysAccept').toString()) : false

        if (map.containsKey('args')) {
            def argsData = map.get('args')
            if (argsData instanceof List) {
                step.args = argsData.collect { it.toString() }
            }
        }

        if (map.containsKey('maxWidth')) {
            step.maxWidth = parseInt(map.get('maxWidth'))
        }
        if (map.containsKey('maxHeight')) {
            step.maxHeight = parseInt(map.get('maxHeight'))
        }

        if (map.containsKey('properties') && map.get('properties') instanceof Map) {
            step.properties = new HashMap<>((Map) map.get('properties'))
        }

        return step
    }

    @CompileDynamic
    private void loadToolsets(String prefix) {
        def toolsetsConfig = getConfigMap("${prefix}.toolsets")
        if (!toolsetsConfig) {
            log.debug("No toolsets config found, using defaults")
            return
        }

        if (!(toolsetsConfig instanceof Map)) {
            return
        }

        Map<String, Map<String, List<StageStep>>> toolsets = [:]

        toolsetsConfig.each { String toolsetName, Object formatsData ->
            if (formatsData instanceof Map) {
                Map<String, List<StageStep>> formatMap = [:]
                formatsData.each { String format, Object stepsData ->
                    if (stepsData instanceof List) {
                        List<StageStep> steps = []
                        stepsData.each { Object stepData ->
                            StageStep step = parseStageStep(stepData)
                            if (step) {
                                steps.add(step)
                            }
                        }
                        if (steps) {
                            formatMap[format] = steps
                        }
                    }
                }
                if (formatMap) {
                    toolsets[toolsetName] = formatMap
                }
            }
        }

        if (toolsets) {
            imageOptimisationConfig.toolsets = toolsets
        }
    }

    @CompileDynamic
    private void loadTools(String prefix) {
        def toolsConfig = getConfigMap("${prefix}.tools")
        if (!toolsConfig) {
            log.debug("No tools config found, using defaults")
            return
        }

        if (!(toolsConfig instanceof Map)) {
            return
        }

        Map<String, Tool> tools = [:]

        toolsConfig.each { String toolName, Object toolData ->
            Tool tool = parseTool(toolData)
            if (tool) {
                tools[toolName] = tool
            }
        }

        if (tools) {
            imageOptimisationConfig.tools = tools
        }
    }

    private Tool parseTool(Object data) {
        if (!(data instanceof Map)) {
            return null
        }

        Map map = (Map) data
        Tool tool = new Tool()

        tool.cmd = map.get('cmd')?.toString()
        tool.type = map.get('type')?.toString() ?: 'process'
        tool.inPlace = map.containsKey('inPlace') ? Boolean.valueOf(map.get('inPlace').toString()) : false
        tool.stdout = map.containsKey('stdout') ? Boolean.valueOf(map.get('stdout').toString()) : false
        tool.className = map.get('className')?.toString()

        if (map.containsKey('fallback')) {
            def fb = map.get('fallback')
            if (fb instanceof List) {
                tool.fallback = fb.collect { it.toString() }
            }
        }

        return tool
    }

    // Helper methods to safely get config values
    private Map getConfigMap(String key) {
        return grailsApplication.config.getProperty(key, Map)
    }

    private String getConfigString(String key, String defaultValue) {
        return grailsApplication.config.getProperty(key, String, defaultValue)
    }

    private boolean getConfigBoolean(String key, boolean defaultValue) {
        return grailsApplication.config.getProperty(key, Boolean, defaultValue)
    }

    private int getConfigInt(String key, int defaultValue) {
        return grailsApplication.config.getProperty(key, Integer, defaultValue)
    }

    private long getConfigLong(String key, long defaultValue) {
        return grailsApplication.config.getProperty(key, Long, defaultValue)
    }

    private static long parseLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue()
        }
        if (value != null) {
            try {
                return Long.parseLong(value.toString())
            } catch (NumberFormatException ignored) {
            }
        }
        return 0L
    }

    private static Integer parseInt(Object value) {
        if (value instanceof Number) {
            return ((Number) value).intValue()
        }
        if (value != null) {
            try {
                return Integer.parseInt(value.toString())
            } catch (NumberFormatException ignored) {
            }
        }
        return null
    }
}

