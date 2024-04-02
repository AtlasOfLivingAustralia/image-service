package au.org.ala.images.utils

import au.org.ala.ws.security.AlaSecurityInterceptor
import au.org.ala.ws.security.client.AlaAuthClient
import au.org.ala.ws.security.profile.AlaOidcUserProfile
import com.nimbusds.oauth2.sdk.Scope
import com.nimbusds.oauth2.sdk.token.BearerAccessToken
import grails.config.Config
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres
import org.flywaydb.core.Flyway
import org.grails.config.PropertySourcesConfig
import org.grails.orm.hibernate.cfg.Settings
import org.pac4j.core.client.BaseClient
import org.pac4j.core.profile.creator.ProfileCreator
import org.pac4j.oidc.credentials.OidcCredentials
import org.slf4j.LoggerFactory
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

import java.lang.reflect.Field
import java.lang.reflect.Modifier

abstract class ImagesIntegrationSpec extends Specification {

    AlaSecurityInterceptor alaSecurityInterceptor
    AlaAuthClient alaAuthClient
    ProfileCreator profileCreator

    @Shared @AutoCleanup EmbeddedPostgres embeddedPostgres = EmbeddedPostgres.builder()
            .setPort(ImagesIntegrationSpec.config.getProperty('dataSource.embeddedPort',  Integer.class, 6543))
            .setCleanDataDirectory(true)
            .start()
    @Shared Flyway flyway = null

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

    /***
     * This method can be used to set values for private final properties
     * @param field
     * @param newValue
     * @param obj
     * @throws Exception
     */
    static void setNewValue(Field field, Object newValue, obj) throws Exception {
        field.setAccessible(true)
        Field modifiersField = Field.class.getDeclaredField("modifiers")
        modifiersField.setAccessible(true)
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL)
        field.set(obj, newValue)
    }

    def setup() {
        def logger = LoggerFactory.getLogger(getClass())
        alaAuthClient = Mock(AlaAuthClient)
        profileCreator = Mock()
        alaAuthClient.getCredentials(_,_) >> Optional.of(new OidcCredentials(userProfile: new AlaOidcUserProfile("1"), accessToken:
                new BearerAccessToken('eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c',
                        2l, new Scope("image-service/write"))))
        alaSecurityInterceptor.alaAuthClient = alaAuthClient
        profileCreator.create(_,_,_) >> Optional.of(new AlaOidcUserProfile("1"))
        setNewValue(BaseClient.class.getDeclaredField("logger"), logger, alaAuthClient)
        setNewValue(BaseClient.class.getDeclaredField("profileCreator"), profileCreator, alaAuthClient)
    }

    void setupSpec() {
        Config config = getConfig()
        // CHANGED added flyway migrate
        this.flyway = Flyway.configure()
                .cleanDisabled(false)
                .table(config.getProperty('flyway.table'))
                .baselineOnMigrate(config.getProperty('flyway.baselineOnMigrate', Boolean))
                .baselineVersion(config.getProperty('flyway.baselineVersion'))
                .outOfOrder(config.getProperty('flyway.outOfOrder', Boolean))
                .placeholders([
                        'imageRoot': config.getProperty('imageservice.imagestore.root'),
                        'exportRoot': config.getProperty('imageservice.imagestore.exportDir', '/data/image-service/exports'),
                        'baseUrl': config.getProperty('grails.serverURL', 'https://devt.ala.org.au/image-service')
                ])
                .locations('db/migration')
                .dataSource(embeddedPostgres.getPostgresDatabase())
                .load()
        flyway.clean()
        flyway.migrate()
        // END CHANGED
    }

    void cleanupSpec() {
        //flyway.clean()
    }

}
