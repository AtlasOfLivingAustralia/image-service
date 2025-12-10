package au.org.ala.images

import grails.boot.GrailsApp
import grails.boot.config.GrailsAutoConfiguration
import grails.core.GrailsApplication
import org.springframework.context.annotation.Bean

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

class Application extends GrailsAutoConfiguration {
    static void main(String[] args) {
        GrailsApp.run(Application, args)
    }

    @Bean
    ExecutorService analyticsExecutor() {
        return Executors.newSingleThreadExecutor()
    }

    @Bean
    StorageOperationsRegistry storageOperationsRegistry() {
        return new StorageOperationsRegistry(this.grailsApplication)
    }
}