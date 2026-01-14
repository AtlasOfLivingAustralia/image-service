package au.org.ala.images.config

import au.org.ala.images.optimisation.ImageOptimTool
import groovy.transform.CompileStatic

@CompileStatic
class Tool {
    String cmd
    String type = 'process' // 'process' or 'java'
    boolean inPlace = false
    boolean stdout = false  // true if tool writes output to stdout instead of a file
    List<String> fallback = []
    // For Java tools
    String className // using 'className' to avoid conflict with Class

    static Tool process(String cmd, boolean inPlace, List<String> fallback = []) {
        Tool tool = new Tool()
        tool.cmd = cmd
        tool.type = 'process'
        tool.inPlace = inPlace
        tool.fallback = fallback
        return tool
    }

    static Tool stdout(String cmd, List<String> fallback = []) {
        Tool tool = new Tool()
        tool.cmd = cmd
        tool.type = 'process'
        tool.stdout = true
        tool.fallback = fallback
        return tool
    }

    static Tool java(Class<? extends ImageOptimTool> clazz) {
        Tool tool = new Tool()
        tool.type = 'java'
        tool.className = clazz.name
        tool.inPlace = false
        return tool
    }

    static Tool java(String className) {
        Tool tool = new Tool()
        tool.type = 'java'
        tool.className = className
        tool.inPlace = false
        return tool
    }
}
