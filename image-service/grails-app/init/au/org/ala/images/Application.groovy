package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import au.org.ala.images.optimisation.CommandExecutor
import au.org.ala.images.optimisation.ProcessCommandExecutor
import au.org.ala.images.thumb.DelegatingImageThumbnailer
import au.org.ala.images.tiling.DelegatingImageTiler
import au.org.ala.images.tiling.ImageTilerConfig
import au.org.ala.images.tiling.TileFormat
import com.google.common.util.concurrent.ThreadFactoryBuilder
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean

import java.awt.Color
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

//@EnableConfigurationProperties(ImageOptimisationConfig)
class Application extends GrailsAutoConfiguration {
    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

//    @Autowired
//    ImageOptimisationConfig imageOptimisationConfig

    @Value('${images.streamingTool:vips}')
    String streamingTool

    @Value('${images.preferJna:true}')
    boolean preferJna

    @Value('${tiling.ioThreads:2}')
    int tilingIoThreads

    @Value('${tiling.levelThreads:2}')
    int tilingLevelThreads

    @Value('${tiling.ioVirtualThreads:true}')
    boolean tilingIoVirtualThreads

    @Bean
    ExecutorService analyticsExecutor() {
        return Executors.newSingleThreadExecutor()
    }

    @Bean
    StorageOperationsRegistry storageOperationsRegistry() {
        return new StorageOperationsRegistry(this.grailsApplication)
    }

    // This is a workaround, instead of using @EnableConfigurationProperties above
    // we create the bean manually then load it using the ImageOptimisationConfigLoader
    // (which is a @Component)
    @Bean
    ImageOptimisationConfig imageOptimisationConfig() {
        return new ImageOptimisationConfig()
    }

    @Bean
    CommandExecutor commandExecutor() {
        return new ProcessCommandExecutor()
    }

    @Bean
    DelegatingImageThumbnailer delegatingImageThumbnailer(CommandExecutor commandExecutor) {
        return new DelegatingImageThumbnailer(commandExecutor, streamingTool, preferJna)
    }

    @Bean
    ExecutorService tilingIoPool() {
        return tilingIoVirtualThreads ? Executors.newVirtualThreadPerTaskExecutor() :
                Executors.newFixedThreadPool(tilingIoThreads, new ThreadFactoryBuilder().setNameFormat("tiling-io-pool-%d").build())
    }

    @Bean
    ExecutorService tilingWorkPool() {
        return Executors.newFixedThreadPool(tilingLevelThreads, new ThreadFactoryBuilder().setNameFormat("tiling-work-pool-%d").build())
    }

    @Bean
    ImageTilerConfig imageTilerConfig(ExecutorService tilingIoPool, ExecutorService tilingWorkPool) {
        def config = new ImageTilerConfig(tilingIoPool, tilingWorkPool, 256, 6, TileFormat.JPEG)
        config.setTileBackgroundColor(new Color(221, 221, 221))
        return config
    }

    @Bean
    DelegatingImageTiler delegatingImageTiler(CommandExecutor commandExecutor, ImageTilerConfig imageTilerConfig) {
        return new DelegatingImageTiler(commandExecutor, imageTilerConfig, streamingTool, preferJna)
    }
}