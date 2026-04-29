package au.org.ala.images.test

import au.org.ala.images.factory.ImageLibraryFactory
import au.org.ala.images.iiif.IiifImageProcessor
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.thumb.IImageThumbnailer
import au.org.ala.images.thumb.ThumbDefinition
import au.org.ala.images.tiling.IImageTiler
import au.org.ala.images.tiling.IOnDemandImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat
import au.org.ala.images.tiling.TilerSink
import au.org.ala.images.util.ByteSinkFactory
import com.google.common.io.ByteSink
import com.google.common.io.ByteSource
import com.google.common.io.Files
import com.google.common.io.Resources
import spock.lang.Specification
import spock.lang.TempDir

import javax.imageio.ImageIO
import java.awt.Color
import java.awt.Graphics2D
import java.awt.image.BufferedImage
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

abstract class AbstractImageLibrarySpec extends Specification {

    // 900×700 deliberately NOT a multiple of the 256 tile size, which forces
    // partial/edge tiles at every zoom level while still producing the same
    // 3-level pyramid [4,2,1] as a 1024×1024 image would via DefaultZoomFactorStrategy.
    //
    // Resulting partial tiles (tileSize = 256):
    //   level 2 (sub=1):  right edge W=132, top edge H=188, corner 132×188
    //   level 1 (sub=2):  right edge W=194, top edge H= 94, corner 194× 94
    //   level 0 (sub=4):  single 225×175 tile (all four quadrants blended)
    //
    // The colour quadrant boundary sits at (QUAD_HALF_W, QUAD_HALF_H) = (450,350).
    // All edge tiles still fall entirely within one colour quadrant, so colour
    // assertions remain exact.
    static final int QUAD_IMG_W  = 900
    static final int QUAD_IMG_H  = 700
    static final int QUAD_HALF_W = 450   // = QUAD_IMG_W / 2
    static final int QUAD_HALF_H = 350   // = QUAD_IMG_H / 2
    static final int TILE_COLOUR_TOLERANCE = 15  // allows for minor resize rounding

    // ── Spock lifecycle ───────────────────────────────────────────────────────

    @TempDir
    File tempDir

    ExecutorService ioExecutor
    ExecutorService levelExecutor

    def setup() {
        ioExecutor = Executors.newFixedThreadPool(2)
        levelExecutor = Executors.newFixedThreadPool(2)
    }

    def cleanup() {
        ioExecutor?.shutdown()
        levelExecutor?.shutdown()
    }

    // ── abstract contract ─────────────────────────────────────────────────────

    abstract ImageLibraryFactory getFactory()
    abstract Map<String, String> getCommands()

    CommandExecutor getCommandExecutor() {
        return Mock(CommandExecutor)
    }

    // ── tests ─────────────────────────────────────────────────────────────────

    def "thumbnailer generates a thumbnail"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def thumbnailer = factory.createThumbnailer(getCommandExecutor(), getCommands(), null)
        if (thumbnailer == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support thumbnailing, skipping"
            return
        }

        def imageResource = "images/audio-icon.png"
        def imageBytes    = Resources.asByteSource(Resources.getResource(imageResource))
        def thumbDef      = new ThumbDefinition(100, true, null, "test-thumb.png")
        def sinkFactory   = new SimpleByteSinkFactory(tempDir)

        when:
        def results = thumbnailer.generateThumbnails(imageBytes, sinkFactory, [thumbDef])

        then:
        results.size() == 1
        results[0].thumbnailName == "test-thumb.png"
        results[0].width > 0
        new File(tempDir, "test-thumb.png").exists()
    }

    def "tiler generates tiles"() {
        given: 'factory + tiler'
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        // Use PNG tiles so colour assertions are not degraded by JPEG compression.
        def config = pngTilerConfig(true)
        def tiler  = factory.createTiler(getCommandExecutor(), config, getCommands(), null)
        if (tiler == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support tiling, skipping"
            return
        }

        and: 'a synthetic 1024×1024 four-quadrant PNG (layout in image coordinates, y=0 at top)'
        // TL = RED   │ TR = GREEN
        // ───────────┼───────────
        // BL = BLUE  │ BR = YELLOW
        File imageFile = createQuadrantImageFile(tempDir)
        File tilesDir  = new File(tempDir, "tiles")
        tilesDir.mkdirs()

        and: 'compute the expected pyramid from the same strategy the config uses'
        int tileSize  = config.tileSize
        int[] pyramid = config.zoomFactorStrategy.getZoomFactors(QUAD_IMG_H, QUAD_IMG_W)
        // pyramid = [4, 2, 1] → 3 levels for a 1024×1024 / 256-px-tile combination

        when: 'the entire image is tiled'
        def results = tiler.tileImage(imageFile, tilesDir)

        then: 'tiling succeeded and the reported level count matches the expected pyramid'
        results.success
        results.zoomLevels == pyramid.length

        and: 'every tile file exists, has the right pixel dimensions, and has the correct quadrant colour'
        for (int level = 0; level < pyramid.length; level++) {
            int sub    = pyramid[level]
            int levelW = (int) Math.ceil(QUAD_IMG_W / (double) sub)
            int levelH = (int) Math.ceil(QUAD_IMG_H / (double) sub)
            int tilesX = (int) Math.ceil(levelW / (double) tileSize)
            int tilesY = (int) Math.ceil(levelH / (double) tileSize)

            for (int x = 0; x < tilesX; x++) {
                for (int y = 0; y < tilesY; y++) {

                    // ── existence ──────────────────────────────────────────
                    File tileFile = new File(tilesDir, "${level}/${x}/${y}.png")
                    assert tileFile.exists() : "Tile file missing: $level/$x/$y"

                    // ── dimensions ─────────────────────────────────────────
                    // TMS convention: y=0 is the BOTTOM row of the level.
                    // tileTopPx / tileBottomPx are in level-pixel space (0 = top of level).
                    int tileTopPx    = Math.max(0,      levelH - (y + 1) * tileSize)
                    int tileBottomPx = levelH - y * tileSize   // always ≤ levelH for valid y
                    int tileLeftPx   = x * tileSize
                    int tileRightPx  = Math.min(levelW, (x + 1) * tileSize)
                    int actualW      = tileRightPx  - tileLeftPx
                    int actualH      = tileBottomPx - tileTopPx
                    int expectedW    = shouldPad(config, actualW, actualH) ? tileSize : actualW
                    int expectedH    = shouldPad(config, actualW, actualH) ? tileSize : actualH

                    BufferedImage tile = ImageIO.read(tileFile)
                     assert tile        != null      : "Could not decode tile: $level/$x/$y"
                     assert tile.width  == expectedW : "Tile $level/$x/$y: width  ${tile.width}  != $expectedW"
                     assert tile.height == expectedH : "Tile $level/$x/$y: height ${tile.height} != $expectedH"

                    // ── colour ─────────────────────────────────────────────
                    // At subsample ≤ 2 each tile's source footprint is ≤ 512×512 px,
                    // which matches exactly one 512×512 quadrant — no blending at borders.
                    // At subsample = 4 (level 0) the single tile blends all four quadrants
                    // so colour sampling is not meaningful there.
                    if (sub <= 2) {
                        int srcCenterX = (tileLeftPx + (actualW as int).intdiv(2)) * sub
                        int srcCenterY = (tileTopPx  + (actualH as int).intdiv(2)) * sub
                        Color expected = quadrantColour(srcCenterX, srcCenterY, QUAD_HALF_W, QUAD_HALF_H)
                        Color actual   = sampleTileColour(tile, actualW, actualH)
                        assert colourClose(actual, expected, TILE_COLOUR_TOLERANCE) :
                            "Tile $level/$x/$y centre colour $actual expected $expected " +
                            "(srcCenterX=$srcCenterX srcCenterY=$srcCenterY sub=$sub)"
                    }

                    assertPngPaddingTransparent(tile, actualW, actualH, config)
                }
            }
        }
    }

    def "iiif processor processes an image"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def processor = factory.createIiifProcessor(getCommandExecutor(), getCommands(), null)
        if (processor == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support IIIF, skipping"
            return
        }

        def imageResource = "images/audio-icon.png"
        def imageBytes    = Resources.asByteSource(Resources.getResource(imageResource))
        def out           = new ByteArrayOutputStream()

        when:
        def result = processor.process(
                imageBytes,
                IiifImageProcessor.Region.full(),
                IiifImageProcessor.Size.width(100, false),
                IiifImageProcessor.Rotation.none(),
                IiifImageProcessor.Quality.DEFAULT,
                IiifImageProcessor.Format.PNG,
                out
        )

        then:
        result != null
        result.width == 100
        out.size() > 0
    }

    def "on-demand tiler generates a tile"() {
        given: 'factory + tiler'
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        // PNG tiles → lossless output → exact colour assertions with a small tolerance
        // for sub-pixel rounding during resize.
        def config = pngTilerConfig(true)
        def tiler  = factory.createOnDemandTiler(getCommandExecutor(), config, getCommands(), null)
        if (tiler == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support on-demand tiling, skipping"
            return
        }

        and: 'a basic level-0 tile from the audio icon'
        def audioStream = Resources.getResource("images/audio-icon.png").openStream()
        def basicSink   = new TilerSink.PathBasedTilerSink(new SimpleByteSinkFactory(tempDir))

        when: 'level 0 tile is generated'
        def level0Result = tiler.generateTile(audioStream, basicSink, 0, 0, 0)

        then: 'level 0 tile is created successfully'
        level0Result != null
        level0Result.success
        new File(tempDir, "0/0/0.png").exists()

        // ── level-1+ validation using a quadrant image ────────────────────────

        when: 'specific tiles at level 1 and level 2 are generated from the quadrant image'
        //
        // Quadrant image layout (900×700, image-space y=0 at top):
        //   TL = RED   │ TR = GREEN   (quadrant boundary: x=450, y=350)
        //   ───────────┼───────────
        //   BL = BLUE  │ BR = YELLOW
        //
        // pyramid = [4,2,1] (same as for a 1024×1024 / 256-px tile image)
        //
        // Level 1 (sub=2, levelW=450, levelH=350 → 2×2 tiles):
        //   (0,0): 256×256  BL→BLUE     (1,0): 194×256  BR→YELLOW  ← right edge
        //   (0,1): 256× 94  TL→RED ← top edge   (1,1): 194×94  TR→GREEN ← corner
        //
        // Level 2 (sub=1, levelW=900, levelH=700 → 4×3 tiles):
        //   (0,0): 256×256  BL→BLUE      (3,0): 132×256  BR→YELLOW  ← right edge
        //   (0,2): 256×188  TL→RED ← top edge   (3,2): 132×188  TR→GREEN ← corner
        File quadFile = createQuadrantImageFile(tempDir)
        File tilesDir = new File(tempDir, "quad-tiles")
        tilesDir.mkdirs()
        def quadSink = new TilerSink.PathBasedTilerSink(new SimpleByteSinkFactory(tilesDir))

        // Level 1: all 4 tiles – 2 of the 4 are edge tiles (partial width/height/both)
        def level1Results = [:]
        [[0,0],[1,0],[0,1],[1,1]].each { coords ->
            new FileInputStream(quadFile).withCloseable { is ->
                level1Results["${coords[0]},${coords[1]}"] = tiler.generateTile(is, quadSink, 1, coords[0], coords[1])
            }
        }

        // Level 2: four tiles chosen to exercise all edge categories
        //   (0,0) interior, (3,0) right-edge, (0,2) top-edge, (3,2) top-right corner
        def level2Results = [:]
        [[0,0],[3,0],[0,2],[3,2]].each { coords ->
            new FileInputStream(quadFile).withCloseable { is ->
                level2Results["${coords[0]},${coords[1]}"] = tiler.generateTile(is, quadSink, 2, coords[0], coords[1])
            }
        }

        then: 'all level 1 and level 2 tiles succeeded and have the correct dimensions and colours'
        int tileSize = config.tileSize

        // ── level 1 ─────────────────────────────────────────────────────────────
        // levelW=450, levelH=350, tileSize=256
        //   right-edge width  = 450 - 256 = 194
        //   top-edge height   = 350 - 256 =  94
        level1Results.values().every { it.success }

        // [x, y, actualW, actualH, expectedColour]
        [[0,0, 256,  256, Color.BLUE  ],   // interior
         [1,0, 194,  256, Color.YELLOW],   // right edge  – partial width
         [0,1, 256,   94, Color.RED   ],   // top edge    – partial height
         [1,1, 194,   94, Color.GREEN ]    // corner edge – partial both
        ].each { spec ->
            int lx = spec[0] as int, ly = spec[1] as int
            int actualW = spec[2] as int, actualH = spec[3] as int
            int expW = shouldPad(config, actualW, actualH) ? tileSize : actualW
            int expH = shouldPad(config, actualW, actualH) ? tileSize : actualH
            Color expColour = spec[4] as Color
            File f = new File(tilesDir, "1/${lx}/${ly}.png")
            assert f.exists()        : "Level-1 tile missing: 1/$lx/$ly"
            BufferedImage tile = ImageIO.read(f)
            assert tile.width  == expW : "Level-1 1/$lx/$ly width  ${tile.width}  != $expW"
            assert tile.height == expH : "Level-1 1/$lx/$ly height ${tile.height} != $expH"
            Color actual = sampleTileColour(tile, actualW, actualH)
            assert colourClose(actual, expColour, TILE_COLOUR_TOLERANCE) :
                "Level-1 1/$lx/$ly centre colour $actual expected $expColour"
            assertPngPaddingTransparent(tile, actualW, actualH, config)
        }

        // ── level 2 ─────────────────────────────────────────────────────────────
        // levelW=900, levelH=700, tileSize=256
        //   right-edge width  = 900 - 3×256 = 132
        //   top-edge height   = 700 - 2×256 = 188
        level2Results.values().every { it.success }

        // [x, y, actualW, actualH, expectedColour]
        [[0,0, 256,  256, Color.BLUE  ],   // interior
         [3,0, 132,  256, Color.YELLOW],   // right edge  – partial width
         [0,2, 256,  188, Color.RED   ],   // top edge    – partial height
         [3,2, 132,  188, Color.GREEN ]    // corner edge – partial both
        ].each { spec ->
            int lx = spec[0] as int, ly = spec[1] as int
            int actualW = spec[2] as int, actualH = spec[3] as int
            int expW = shouldPad(config, actualW, actualH) ? tileSize : actualW
            int expH = shouldPad(config, actualW, actualH) ? tileSize : actualH
            Color expColour = spec[4] as Color
            File f = new File(tilesDir, "2/${lx}/${ly}.png")
            assert f.exists()        : "Level-2 tile missing: 2/$lx/$ly"
            BufferedImage tile = ImageIO.read(f)
            assert tile.width  == expW : "Level-2 2/$lx/$ly width  ${tile.width}  != $expW"
            assert tile.height == expH : "Level-2 2/$lx/$ly height ${tile.height} != $expH"
            Color actual = sampleTileColour(tile, actualW, actualH)
            assert colourClose(actual, expColour, TILE_COLOUR_TOLERANCE) :
                "Level-2 2/$lx/$ly centre colour $actual expected $expColour"
            assertPngPaddingTransparent(tile, actualW, actualH, config)
        }
    }

    def "on-demand tiler preserves partial edge tiles when padding disabled"() {
        given:
        def factory = getFactory()
        if (!factory.isAvailable(getCommands())) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} not available, skipping test"
            return
        }

        def config = pngTilerConfig(false)
        def tiler  = factory.createOnDemandTiler(getCommandExecutor(), config, getCommands(), null)
        if (tiler == null) {
            println "[DEBUG_LOG] Factory ${factory.implementationName} does not support on-demand tiling, skipping"
            return
        }

        File quadFile = createQuadrantImageFile(tempDir)
        File tilesDir = new File(tempDir, "quad-tiles-unpadded")
        tilesDir.mkdirs()
        def quadSink = new TilerSink.PathBasedTilerSink(new SimpleByteSinkFactory(tilesDir))

        when:
        [[1,1,1,194, 94, Color.GREEN ],
         [2,3,0,132,256, Color.YELLOW],
         [2,0,2,256,188, Color.RED   ]].each { spec ->
            new FileInputStream(quadFile).withCloseable { is ->
                tiler.generateTile(is, quadSink, spec[0] as int, spec[1] as int, spec[2] as int)
            }
        }

        then:
        [[1,1,1,194, 94, Color.GREEN ],
         [2,3,0,132,256, Color.YELLOW],
         [2,0,2,256,188, Color.RED   ]].each { spec ->
            int level = spec[0] as int
            int x = spec[1] as int
            int y = spec[2] as int
            int actualW = spec[3] as int
            int actualH = spec[4] as int
            Color expected = spec[5] as Color
            File tileFile = new File(tilesDir, "${level}/${x}/${y}.png")
            assert tileFile.exists(): "Tile missing: ${level}/${x}/${y}"
            BufferedImage tile = ImageIO.read(tileFile)
            assert tile.width == actualW
            assert tile.height == actualH
            assert colourClose(sampleTileColour(tile, actualW, actualH), expected, TILE_COLOUR_TOLERANCE)
        }
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    /**
     * Writes a 900×700 PNG to {@code dir/quadrant.png} with four solid-colour quadrants.
     * The colour boundary sits at (450, 350) – the exact midpoint of each axis:
     *   TL = RED  (x<450, y<350)  │  TR = GREEN  (x≥450, y<350)
     *   ────────────────────────────────────────────────────────
     *   BL = BLUE (x<450, y≥350)  │  BR = YELLOW (x≥450, y≥350)
     *
     * 900×700 deliberately does NOT divide evenly by the 256 px tile size, so the
     * tiler is forced to produce partial/edge tiles (132 px wide at x-right,
     * 188 px tall at y-top at level 2; 194 px wide and 94 px tall at level 1).
     * All edge tiles still land entirely within one colour quadrant, so colour
     * assertions remain exact.
     */
    protected File createQuadrantImageFile(File dir) {
        BufferedImage img = new BufferedImage(QUAD_IMG_W, QUAD_IMG_H, BufferedImage.TYPE_INT_RGB)
        Graphics2D g = img.createGraphics()
        g.color = Color.RED;    g.fillRect(0,          0,          QUAD_HALF_W, QUAD_HALF_H)  // top-left
        g.color = Color.GREEN;  g.fillRect(QUAD_HALF_W, 0,          QUAD_HALF_W, QUAD_HALF_H)  // top-right
        g.color = Color.BLUE;   g.fillRect(0,          QUAD_HALF_H, QUAD_HALF_W, QUAD_HALF_H)  // bottom-left
        g.color = Color.YELLOW; g.fillRect(QUAD_HALF_W, QUAD_HALF_H, QUAD_HALF_W, QUAD_HALF_H) // bottom-right
        g.dispose()
        File file = new File(dir, "quadrant.png")
        ImageIO.write(img, "png", file)
        return file
    }

    /**
     * Returns the expected solid colour for source-image pixel (srcX, srcY).
     * Image coordinates: y=0 at top, x=0 at left.
     * {@code halfW} / {@code halfH} are the x and y quadrant boundaries respectively.
     */
    protected static Color quadrantColour(int srcX, int srcY, int halfW, int halfH) {
        if (srcX < halfW && srcY < halfH) return Color.RED     // top-left
        if (srcX >= halfW && srcY < halfH) return Color.GREEN  // top-right
        if (srcX < halfW && srcY >= halfH) return Color.BLUE   // bottom-left
        return Color.YELLOW                                     // bottom-right
    }

    protected ImageTilerConfig pngTilerConfig(boolean padTiles) {
        new ImageTilerConfig(ioExecutor, levelExecutor, 256, 6, TileFormat.PNG, new Color(221, 221, 221), padTiles)
    }

    protected static boolean shouldPad(ImageTilerConfig config, int actualWidth, int actualHeight) {
        config.padTiles && (actualWidth < config.tileSize || actualHeight < config.tileSize)
    }

    protected static Color sampleTileColour(BufferedImage tile, int actualWidth, int actualHeight) {
        int sampleX = Math.max(0, Math.min(actualWidth - 1, actualWidth.intdiv(2)))
        int sampleY = Math.max(0, Math.min(tile.height - 1, tile.height - actualHeight + actualHeight.intdiv(2)))
        new Color(tile.getRGB(sampleX, sampleY), true)
    }

    protected static void assertPngPaddingTransparent(BufferedImage tile, int actualWidth, int actualHeight, ImageTilerConfig config) {
        if (config.tileFormat != TileFormat.PNG || !shouldPad(config, actualWidth, actualHeight)) {
            return
        }

        if (actualWidth < tile.width) {
            assert new Color(tile.getRGB(tile.width - 1, tile.height - 1), true).alpha == 0
        }
        if (actualHeight < tile.height) {
            assert new Color(tile.getRGB(0, 0), true).alpha == 0
        }
    }

    /** True when every RGB channel of {@code actual} is within {@code tolerance} of {@code expected}. */
    protected static boolean colourClose(Color actual, Color expected, int tolerance) {
        Math.abs(actual.red   - expected.red)   <= tolerance &&
        Math.abs(actual.green - expected.green) <= tolerance &&
        Math.abs(actual.blue  - expected.blue)  <= tolerance
    }

    // ── inner classes ─────────────────────────────────────────────────────────

    static class SimpleByteSinkFactory implements ByteSinkFactory {
        File dir
        SimpleByteSinkFactory(File dir) { this.dir = dir }
        @Override void prepare() {}
        @Override ByteSink getByteSinkForNames(String... names) {
            File file = dir
            for (String name : names) {
                file = new File(file, name)
            }
            file.parentFile.mkdirs()
            return Files.asByteSink(file)
        }
    }

}
