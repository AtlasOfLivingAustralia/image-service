package au.org.ala.images.optimisation
/**
 * SPI for Java-based optimisation tools that run in-process instead of shelling out.
 */
interface ImageOptimTool {
    /**
     * Called once per tool definition to initialise with static tool configuration from `images.optimisation.tools`.
     */
    void init(Map toolConfig)

    /**
     * Execute the tool logic.
     * @param in input image file (never null)
     * @param out output image file path to write (may be same as in if inPlace tool)
     * @param stepConfig the step block from toolset (e.g., maxWidth/maxHeight, outputFormat, lossy flags)
     * @param context runtime info map (e.g., format, contentType, workDir)
     * @return ExecResult with exitCode 0 on success and optional stdout/stderr for logging
     */
    CommandExecutor.ExecResult run(File inputFile, File outputFile, Map stepConfig, Map context)
}