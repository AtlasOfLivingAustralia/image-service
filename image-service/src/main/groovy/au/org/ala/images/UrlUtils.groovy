package au.org.ala.images

class UrlUtils {
    static String maskCredentials(String value, boolean isAdmin = false) {
        if (!value) return ''
        if (isAdmin) return value
        try {
            URI uri = new URI(value)
            if (uri.userInfo) {
                String maskedUserInfo = uri.userInfo.contains(":") ? "****:****" : "****"
                URI maskedUri = new URI(
                        uri.scheme,
                        maskedUserInfo,
                        uri.host,
                        uri.port,
                        uri.path,
                        uri.query,
                        uri.fragment)
                return maskedUri.toString()
            }
            return value
        } catch (Throwable ignore) {
            try {
                def pattern = /(\w+:\/\/)([^\/@]+)@/
                return value.replaceAll(pattern) { all, scheme, creds ->
                    def repl = creds?.toString()?.contains(':') ? '****:****' : '****'
                    return "${scheme}${repl}@"
                }
            } catch (Throwable ignored) {
                return value
            }
        }
    }

    static String stripCredentials(String value) {
        if (!value) return ''
        try {
            URI uri = new URI(value)
            if (uri.userInfo) {
                String maskedUserInfo = uri.userInfo.contains(":") ? "****:****" : "****"
                URI maskedUri = new URI(
                        uri.scheme,
                        null,
                        uri.host,
                        uri.port,
                        uri.path,
                        uri.query,
                        uri.fragment)
                return maskedUri.toString()
            }
            return value
        } catch (Throwable ignore) {
            try {
                def pattern = /(\w+:\/\/)([^\/@]+)@/
                return value.replaceAll(pattern) { all, scheme, creds ->
                    return "${scheme}"
                }
            } catch (Throwable ignored) {
                return value
            }
        }
    }
}
