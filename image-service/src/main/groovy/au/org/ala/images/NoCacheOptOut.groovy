package au.org.ala.images

import java.lang.annotation.ElementType
import java.lang.annotation.Retention
import java.lang.annotation.RetentionPolicy
import java.lang.annotation.Target

/**
 * Opt out of global no-cache headers applied by the cache-control interceptor.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target([ElementType.TYPE, ElementType.METHOD])
@interface NoCacheOptOut {
}
