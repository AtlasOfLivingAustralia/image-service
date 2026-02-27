package au.org.ala.images

import au.org.ala.images.config.ImageOptimisationConfig
import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

//@EnableConfigurationProperties(ImageOptimisationConfig)
class Application extends GrailsAutoConfiguration {
    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

//    @Autowired
//    ImageOptimisationConfig imageOptimisationConfig

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
}