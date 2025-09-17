package au.org.ala.images.utils

import au.org.ala.ws.security.AlaSecurityInterceptor
import au.org.ala.ws.security.client.AlaAuthClient
import au.org.ala.ws.security.client.AlaDirectClient
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
import java.lang.reflect.Method
import java.lang.reflect.Modifier

abstract class ImagesIntegrationSpec extends Specification {

    AlaSecurityInterceptor alaSecurityInterceptor
    AlaDirectClient alaAuthClient
    ProfileCreator profileCreator

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
        Method getDeclaredFields0 = Class.class.getDeclaredMethod("getDeclaredFields0", boolean.class)
        getDeclaredFields0.setAccessible(true)
//        Field modifiersField = Field.class.getDeclaredField("modifiers")
        Field[] fields = (Field[]) getDeclaredFields0.invoke(Field.class, false)
        Field modifiersField = null
        for (Field each : fields) {
            if ("modifiers".equals(each.getName())) {
                modifiersField = each;
                break;
            }
        }
        modifiersField.setAccessible(true)
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL)
        field.set(obj, newValue)
    }

    def setupSpec() {
        System.setProperty("http.agent", "ala-image-service/4.0")
    }

    def setup() {
        def logger = LoggerFactory.getLogger(getClass())
        alaAuthClient = Mock(AlaDirectClient)
        profileCreator = Mock()
        def creds = new OidcCredentials(
                userProfile: new AlaOidcUserProfile("1"),
                accessTokenObject: new BearerAccessToken(
                        'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c',
                        2l,
                        new Scope("image-service/write")
                )
        )
        alaAuthClient.getCredentials(_) >> Optional.of(creds)
        alaAuthClient.validateCredentials(_ , _) >> Optional.of(creds)
        alaAuthClient.internalValidateCredentials(_ , _) >> Optional.of(creds)
        alaSecurityInterceptor.clientList = [alaAuthClient]
        profileCreator.create(_,_) >> Optional.of(new AlaOidcUserProfile("1"))
        setNewValue(BaseClient.class.getDeclaredField("logger"), logger, alaAuthClient)
        setNewValue(BaseClient.class.getDeclaredField("profileCreator"), profileCreator, alaAuthClient)
    }

}
