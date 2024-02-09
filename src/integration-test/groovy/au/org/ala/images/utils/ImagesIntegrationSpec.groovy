package au.org.ala.images.utils

import grails.config.Config
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.grails.config.PropertySourcesConfig
import org.grails.orm.hibernate.cfg.Settings
import org.springframework.boot.env.PropertySourceLoader
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.MutablePropertySources
import org.springframework.core.env.PropertySource
import org.springframework.core.io.DefaultResourceLoader
import org.springframework.core.io.Resource
import org.springframework.core.io.ResourceLoader
import org.springframework.core.io.support.SpringFactoriesLoader
import spock.lang.AutoCleanup
import spock.lang.Shared
import spock.lang.Specification

abstract class ImagesIntegrationSpec extends Specification {

    @Shared @AutoCleanup EmbeddedPostgres embeddedPostgres = EmbeddedPostgres.builder()
            .setPort(ImagesIntegrationSpec.config.getProperty('dataSource.embeddedPort',  Integer.class, 6543))
            .setCleanDataDirectory(true)
            .start()

    static Config getConfig() { // CHANGED extracted from setupSpec so postgresRule can access

        List<PropertySourceLoader> propertySourceLoaders = SpringFactoriesLoader.loadFactories(PropertySourceLoader.class, ImagesIntegrationSpec.class.getClassLoader())
        ResourceLoader resourceLoader = new DefaultResourceLoader()
        MutablePropertySources propertySources = new MutablePropertySources()
        PropertySourceLoader ymlLoader = propertySourceLoaders.find { it.getFileExtensions().toList().contains("yml") }
        if (ymlLoader) {
            load(resourceLoader, ymlLoader, "application.yml").each {
                propertySources.addLast(it)
            }
        }
        PropertySourceLoader groovyLoader = propertySourceLoaders.find { it.getFileExtensions().toList().contains("groovy") }
        if (groovyLoader) {
            load(resourceLoader, groovyLoader, "application.groovy").each {
                propertySources.addLast(it)
            }
        }
        propertySources.addFirst(new MapPropertySource("defaults", getConfiguration()))
        return new PropertySourcesConfig(propertySources)
    }

    // Changed: Made static for getConfig()
    private static List<PropertySource> load(ResourceLoader resourceLoader, PropertySourceLoader loader, String filename) {
        if (canLoadFileExtension(loader, filename)) {
            Resource appYml = resourceLoader.getResource(filename)
            return loader.load(appYml.getDescription(), appYml) as List<PropertySource>
        } else {
            return Collections.emptyList()
        }
    }

    // Changed: Made static for getConfig()
    private static boolean canLoadFileExtension(PropertySourceLoader loader, String name) {
        return Arrays
                .stream(loader.fileExtensions)
                .map { String extension -> extension.toLowerCase() }
                .anyMatch { String extension -> name.toLowerCase().endsWith(extension) }
    }

    /**
     * @return The configuration
     */
    static Map getConfiguration() { // changed to static
        Collections.singletonMap(Settings.SETTING_DB_CREATE,  (Object) "validate") // CHANGED from 'create-drop' to 'validate'
    }

}
