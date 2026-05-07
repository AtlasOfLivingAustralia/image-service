package au.org.ala.images.config

import groovy.transform.CompileStatic

@CompileStatic
class Stage {
    String name
    Filter filter
    List<StageStep> steps
    String toolsRef // Reference to a toolset by name
    boolean allowLossy = true

    static Stage resizeLarge() {
        Stage stage = new Stage()
        stage.name = 'resize-large'
        stage.filter = Filter.minPixels(16_000_000).contentTypes(['image/'])
        stage.toolsRef = 'resize4000'
        stage.allowLossy = true
        return stage
    }

    static Stage optimPass1() {
        Stage stage = new Stage()
        stage.name = 'optim-pass1'
        stage.filter = Filter.minBytes(1_000_000)
        stage.toolsRef = 'optimConservative'
        stage.allowLossy = false
        return stage
    }

    static Stage optimPass2() {
        Stage stage = new Stage()
        stage.name = 'optim-pass2'
        stage.filter = Filter.minBytes(1_000_000)
        stage.toolsRef = 'optimConservative'
        stage.allowLossy = false
        return stage
    }

    static Stage optimAggressive() {
        Stage stage = new Stage()
        stage.name = 'optim-aggressive'
        stage.filter = Filter.minBytes(1_000_000)
        stage.toolsRef = 'optimAggressive'
        stage.allowLossy = true
        return stage
    }
}
