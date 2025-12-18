package au.org.ala.images.config

import au.org.ala.images.optimisation.ImageResizeTool
import groovy.transform.CompileStatic
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.NestedConfigurationProperty

@CompileStatic
@ConfigurationProperties(prefix = 'images.optimisation')
class ImageOptimisationConfig {
    public static final String ALL_FORMATS = 'all'

    boolean enabled = true
    String tempDir = System.getProperty('java.io.tmpdir')
    int timeoutSecondsPerTool = 30
    long skipThresholdBytes = 50_000 // 50KB default skip
    int maxLogChars = 2000

    // Named pipelines for stages - Map of pipeline name to list of stages
    @NestedConfigurationProperty
    Map<String, List<Stage>> stages = [
            default: [
                    Stage.resizeLarge(),
                    Stage.optimPass1(),
                    Stage.optimPass2(),
                    Stage.optimAggressive()
            ]
    ]

    // Reusable named toolsets
    // Structure: Map<toolset name, Map<format or '*', List<StageStep>>>
    @NestedConfigurationProperty
    Map<String, Map<String, List<StageStep>>> toolsets = [
            resize4000: [
                    (ALL_FORMATS): [
                            StageStep.lossy('convert', ['%IN%', '-resize', '4000x4000>', '%OUT%']).maxSize(4000, 4000).property('useScalr', true)
                    ]
            ],
            jpegConvert: [
                    (ALL_FORMATS): [
                            StageStep.lossy('convert', [
                                    '%IN%', '-auto-orient', '-colorspace', 'sRGB', '-strip',
                                    '-sampling-factor', '4:2:0', '-interlace', 'Plane',
                                    '-quality', '82', '%OUT%'
                            ]).outputFormat('jpeg'),
                            StageStep.of('jpegtran', ['-copy', 'none', '-optimize', '-progressive']),
                            StageStep.of('jpegoptim', ['--strip-all', '--all-progressive', '--max=85'])
                    ]
            ],
            optimConservative: [
                    jpeg: [
                            StageStep.of('jpegtran', ['-copy', 'none', '-optimize', '-progressive']),
                            StageStep.of('jpegoptim', ['--strip-all', '--all-progressive', '--max=85'])
                    ],
                    png: [
                            StageStep.of('oxipng', ['-o', '3', '--strip', 'all']),
                            StageStep.of('zopflipng', ['-y', '-m'])
                    ],
                    gif: [
                            StageStep.of('gifsicle', ['-O3'])
                    ],
                    svg: [
                            StageStep.of('svgo', ['--multipass'])
                    ],
                    webp: [
                            StageStep.of('webpmux', ['-strip', '%IN%', '-o', '%OUT%'])
                    ]
            ],
            optimAggressive: [
                    jpeg: [
                            StageStep.lossy('mozjpeg', ['-quality', '75', '-optimize', '-progressive', '%IN%'])
                    ],
                    png: [
                            StageStep.lossy('pngquant', ['--quality=60-80', '--speed=1', '--force', '--output=%OUT%', '%IN%'])
                    ],
                    webp: [
                            StageStep.lossy('cwebp', ['-mt', '-q', '75', '%IN%', '-o', '%OUT%'])
                    ],
                    avif: [
                            StageStep.lossy('avifenc', ['-q', '28', '-s', '4', '--jobs', '0', '%IN%', '%OUT%'])
                    ]
            ]
    ]

    /**
     * Get steps for a toolset and format. Checks format-specific first, then falls back to '*'.
     */
    List<StageStep> getToolsetSteps(String toolsetName, String format) {
        Map<String, List<StageStep>> toolset = toolsets[toolsetName]
        if (!toolset) return []
        // Try format-specific first
        List<StageStep> formatSpecific = toolset[format]
        if (formatSpecific) return formatSpecific
        // Fall back to catch-all
        return toolset[ALL_FORMATS] ?: []
    }

    // Tool definitions
    @NestedConfigurationProperty
    Map<String, Tool> tools = [
            // JPEG
            jpegtran: Tool.stdout('jpegtran'),
            jpegoptim: Tool.process('jpegoptim', true),
            mozjpeg: Tool.process('cjpeg', false),
            // Java SPI example tool
            javaResize: Tool.java(ImageResizeTool),
            // PNG
            oxipng: Tool.process('oxipng', true, ['optipng']),
            zopflipng: Tool.process('zopflipng', false),
            pngquant: Tool.process('pngquant', false),
            optipng: Tool.process('optipng', true),
            // GIF/SVG
            gifsicle: Tool.process('gifsicle', true),
            svgo: Tool.process('svgo', true),
            // ImageMagick 7 vs 6
            magick: Tool.process('magick', false, ['convert']),
            convert: Tool.process('convert', false),
            // WebP / AVIF
            cwebp: Tool.process('cwebp', false),
            webpmux: Tool.process('webpmux', false),
            avifenc: Tool.process('avifenc', false)
    ]
}
