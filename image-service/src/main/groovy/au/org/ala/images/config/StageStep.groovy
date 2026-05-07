package au.org.ala.images.config

import groovy.transform.CompileStatic

@CompileStatic
class StageStep {
    String tool // Tool name reference
    boolean lossy = false
    String outputFormat
    boolean acceptLarger = false
    boolean alwaysAccept = false
    List<String> args = []
    // Additional properties for Java tools
    Integer maxWidth
    Integer maxHeight
    // Generic properties map for tool-specific configuration (e.g., useScalr, quality, etc.)
    Map<String, Object> properties = [:]

    static StageStep of(String toolName, List<String> args = []) {
        StageStep step = new StageStep()
        step.tool = toolName
        step.args = args
        return step
    }

    static StageStep lossy(String toolName, List<String> args = []) {
        StageStep step = new StageStep()
        step.tool = toolName
        step.lossy = true
        step.args = args
        return step
    }

    StageStep outputFormat(String format) {
        this.outputFormat = format
        return this
    }

    StageStep acceptLarger(boolean value = true) {
        this.acceptLarger = value
        return this
    }

    StageStep alwaysAccept(boolean value = true) {
        this.alwaysAccept = value
        return this
    }

    StageStep maxSize(int width, int height) {
        this.maxWidth = width
        this.maxHeight = height
        return this
    }

    StageStep property(String key, Object value) {
        this.properties[key] = value
        return this
    }

    StageStep properties(Map<String, Object> props) {
        if (props) {
            this.properties.putAll(props)
        }
        return this
    }
}
