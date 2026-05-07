package au.org.ala.images

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@interface SearchableProperty {
    CriteriaValueType valueType() default CriteriaValueType.StringDirectEntry;
    String units() default "";
    String description() default "";

}
