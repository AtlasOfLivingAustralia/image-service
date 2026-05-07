package au.org.ala.images

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

@Retention(RetentionPolicy.RUNTIME)
@Target([ElementType.METHOD, ElementType.FIELD])
public @interface ImageServiceSetting {
    String name()
    String defaultValue()
    String description()
}
