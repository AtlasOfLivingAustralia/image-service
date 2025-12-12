package au.org.ala.images

import au.org.ala.images.metadata.MetadataExtractor
import com.google.common.io.ByteSource
import dev.failsafe.Failsafe
import dev.failsafe.RetryPolicy
import dev.failsafe.function.CheckedSupplier
import grails.gorm.transactions.NotTransactional
import grails.gorm.transactions.Transactional
import groovy.util.logging.Slf4j
import org.apache.commons.io.FilenameUtils
import org.springframework.beans.factory.annotation.Value

import javax.annotation.PostConstruct
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse as JavaHttpResponse
import java.time.Duration

import static grails.web.http.HttpHeaders.USER_AGENT
import static java.nio.file.Files.createTempFile

/**
 * Service for downloading images from URLs with redirect handling, retry logic,
 * and security checks. Supports both URLConnection (for compatibility and s3:// URLs)
 * and java.net.http.HttpClient (for better performance with HTTP/HTTPS URLs).
 */
@Slf4j
class DownloadService {

    @Value('${http.default.readTimeoutMs:10000}')
    int readTimeoutMs = 10000 // 10 seconds

    @Value('${http.default.connectTimeoutMs:5000}')
    int connectTimeoutMs = 5000 // 5 seconds

    @Value('${http.default.user-agent:}')
    String userAgent

    @Value('${skin.orgNameShort:ALA}')
    String orgNameShort

    @Value('${info.app.name:image-service}')
    String appName

    @Value('${info.app.version:NaN}')
    String version

    @Value('${batch.upload.file.threshold:10485760}')
    long fileCacheThreshold = 1024 * 1024 * 10 // 10MB

    @Value('${http.retry.maxAttempts:3}')
    int maxHttpRetryAttempts = 3

    @Value('${http.retry.initialDelayMs:1000}')
    long httpRetryInitialDelayMs = 1000

    @Value('${http.retry.maxDelayMs:30000}')
    long httpRetryMaxDelayMs = 30000

    @Value('${http.redirect.maxFollows:5}')
    int maxHttpRedirectFollows = 5

    @Value('${http.redirect.sensitiveParams:token,key,secret,apikey,api_key,password,pwd,auth}')
    Set<String> sensitiveParams

    // The following 3 settings should probably be enabled but they default to false for backward compatibility
    @Value('${http.redirect.disallowHttpToHttpDifferentHost:false}')
    boolean disallowHttpToHttpDifferentHost = false

    @Value('${http.redirect.disallowHttpToHttpUserInfo:false}')
    boolean disallowHttpToHttpUserInfo = false

    @Value('${http.redirect.disallowHttpToSensitiveParams:false}')
    boolean disallowHttpToHttpSensitiveParams = false

    @Value('${http.useHttpClient:true}')
    boolean useHttpClient = true

    private HttpClient httpClient

    @PostConstruct
    void init() {
        if (useHttpClient) {
            httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(connectTimeoutMs))
                .followRedirects(HttpClient.Redirect.NEVER) // We handle redirects manually
                .build()
            log.info("Using java.net.http.HttpClient for HTTP/HTTPS requests")
        } else {
            log.info("Using URLConnection for all requests")
        }
    }

    /**
     * @deprecated Use createHttpResponse with URI instead
     */
    @Deprecated
    HttpResponse createHttpResponse(URL url, String originalUrl) {
        createHttpResponse(url.toURI(), originalUrl)
    }

    /**
     * Create an HttpResponse from a URL with redirect handling.
     * This is the preferred method for new code as it works with both HttpClient and URLConnection.
     */
    HttpResponse createHttpResponse(URI uri, String originalUrl) {
        if (useHttpClient) {
            // Use HttpClient for http/https
            if (uri.scheme == 'http' || uri.scheme == 'https') {
                return createHttpResponseWithHttpClient(uri, originalUrl)
            }
            // Use S3HttpResponse for s3:// URLs
            else if (uri.scheme == 's3') {
                return new S3HttpResponse(uri)
            }
        }

        // Fall back to URLConnection for other protocols or when HttpClient is disabled
        def conn = createConnectionWithRedirects(uri.toURL(), originalUrl)
        return new UrlConnectionHttpResponse(conn)
    }

    /**
     * Create an HttpResponse using HttpClient with redirect handling
     */
    private HttpResponse createHttpResponseWithHttpClient(URI uri, String originalUrl) {
        def response = createHttpResponseWithHttpClient(uri, originalUrl, 'GET', InputStream)
        return new HttpClientHttpResponse(response)
    }

    private <T> java.net.http.HttpResponse<T> createHttpResponseWithHttpClient(URI uri, String originalUrl, String method, Class<T> clazz) {
        def currentUri = uri
        def visitedUrls = new HashSet<String>()
        def redirectCount = 0

        while (redirectCount < maxHttpRedirectFollows) {
            if (visitedUrls.contains(currentUri.toString())) {
                throw new IOException("Redirect loop detected for URL: ${originalUrl}")
            }
            visitedUrls.add(currentUri.toString())

            def request = HttpRequest.newBuilder()
                .uri(currentUri)
                .timeout(Duration.ofMillis(readTimeoutMs))
                .header("User-Agent", getUserAgent())
                .method(method, HttpRequest.BodyPublishers.noBody())
                .build()

            java.net.http.HttpResponse.BodyHandler<T> bodyHandler
            switch(clazz) {
                case InputStream:
                    bodyHandler = JavaHttpResponse.BodyHandlers.ofInputStream()
                    break
                case String:
                    bodyHandler = JavaHttpResponse.BodyHandlers.ofString()
                    break
                case Void:
                    bodyHandler = JavaHttpResponse.BodyHandlers.discarding()
                    break
                default:
                    throw new IllegalArgumentException("Unsupported response body type: ${clazz}")
            }
            def response = httpClient.send(request, bodyHandler)
            int statusCode = response.statusCode()

            // Check for redirect status codes
            if (statusCode in [301, 302, 303, 307, 308]) {
                def location = response.headers().firstValue("Location").orElse(null)
                if (location) {
                    URI newUri = currentUri.resolve(location) // Handle relative URLs

                    // Security check
                    if (shouldFollowRedirect(currentUri, newUri)) {
                        log.debug("Following redirect from ${currentUri} to ${newUri}")
                        currentUri = newUri
                        redirectCount++
                        continue
                    } else {
                        throw new IOException("Unsafe redirect from ${currentUri} to ${newUri}")
                    }
                }
            }

            // Not a redirect, return the response
            return response
        }

        throw new IOException("Too many redirects (${maxHttpRedirectFollows}) for URL: ${originalUrl}")
    }

    /**
     * @deprecated Use createByteSourceFromUrl with URI instead
     */
    @Deprecated
    CloseableByteSource createByteSourceFromUrl(URL url, String extension, String imageUrl) {
        createByteSourceFromUrl(url.toURI(), extension, imageUrl)
    }

    /**
     * Create a ByteSource from a URL with redirect handling and retry logic
     */
    CloseableByteSource createByteSourceFromUrl(URI uri, String extension, String imageUrl) {
        def response = createHttpResponse(uri, imageUrl)
        return createByteSourceFromHttpResponse(response, extension, imageUrl)
    }

    /**
     * Create a ByteSource from an HttpResponse.
     * @deprecated Use createByteSourceFromHttpResponse instead
     */
    @Deprecated
    CloseableByteSource createByteSourceFromConnection(URLConnection conn, String extension, String imageUrl) {
        return createByteSourceFromHttpResponse(new UrlConnectionHttpResponse(conn), extension, imageUrl)
    }

    /**
     * Create a ByteSource from an HttpResponse. The byte source is guaranteed to be reusable.
     * Includes retry logic for 5xx errors and proper error handling for 4xx errors.
     */
    CloseableByteSource createByteSourceFromHttpResponse(HttpResponse response, String extension, String imageUrl) {
        // Build retry policy for 5xx errors with exponential backoff
        def retryPolicy = RetryPolicy.builder()
            .handle(ServerErrorException.class)
            .withMaxRetries(maxHttpRetryAttempts)
            .withBackoff(
                    Duration.ofMillis(httpRetryInitialDelayMs),
                    Duration.ofMillis(httpRetryMaxDelayMs)
            )
            .onRetry({ e ->
                log.info("Retrying after server error (attempt ${e.attemptCount}/${maxHttpRetryAttempts}): ${e.lastException?.message}")
            })
            .build()

        return Failsafe.with(retryPolicy).get(() -> {
            int statusCode = response.getStatusCode()

            if (statusCode >= 400 && statusCode < 500) {
                // Client error - don't retry
                String errorMessage = "HTTP ${statusCode} error for URL: ${imageUrl}"
                try {
                    def errorStream = response.getErrorStream()
                    if (errorStream) {
                        String errorBody = errorStream.text
                        if (errorBody && errorBody.length() < 500) {
                            errorMessage += " - ${errorBody}"
                        }
                    }
                } catch (Exception ignored) {
                    // Ignore errors reading error stream
                }
                throw new ClientErrorException(errorMessage, imageUrl, statusCode)
            } else if (statusCode >= 500) {
                // Server error - will be retried by retry policy
                String errorMessage = "HTTP ${statusCode} server error for URL: ${imageUrl}"
                throw new ServerErrorException(errorMessage, imageUrl, statusCode)
            } else if (statusCode < 200 || statusCode >= 300) {
                // Other non-success status
                throw new HttpImageUploadException("HTTP ${statusCode} for URL: ${imageUrl}", imageUrl, statusCode)
            }

            // Status is OK, proceed with downloading
            ByteSource byteSource
            File cacheFile

            long length = response.getContentLength()

            if (length > fileCacheThreshold || length == -1) {
                extension = extension ?: FilenameUtils.getExtension(imageUrl.toURI().getPath())
                cacheFile = createTempFile("image", extension ?: "jpg").toFile()
                cacheFile.deleteOnExit()
                cacheFile << response.getInputStream()
                byteSource = new CloseableByteSource(cacheFile)
            } else {
                byteSource = new CloseableByteSource(response.getInputStream().bytes)
            }
            return byteSource
        } as CheckedSupplier<CloseableByteSource>)
    }

    /**
     * Create a connection with redirect handling and security checks
     * @deprecated Use createHttpResponse instead for new code
     */
    @Deprecated
    URLConnection createConnectionWithRedirects(URL url, String originalUrl) {
        def currentUrl = url
        def currentUri = url.toURI()
        def visitedUrls = new HashSet<String>()
        def redirectCount = 0

        while (redirectCount < maxHttpRedirectFollows) {
            if (visitedUrls.contains(currentUrl.toString())) {
                throw new IOException("Redirect loop detected for URL: ${originalUrl}")
            }
            visitedUrls.add(currentUrl.toString())

            def conn = createConnection(currentUrl)

            // For HTTP connections, check for redirects manually
            if (conn instanceof HttpURLConnection) {
                HttpURLConnection httpConn = (HttpURLConnection) conn
                httpConn.instanceFollowRedirects = false

                int statusCode = httpConn.getResponseCode()

                // Check for redirect status codes
                if (statusCode in [301, 302, 303, 307, 308]) {
                    String location = httpConn.getHeaderField("Location")
                    if (location) {
                        URI newUri = currentUri.resolve(location) // Handle relative URLs
                        URL newUrl = newUri.toURL() // Handle relative URLs

                        // Security check: only follow redirects from HTTP to HTTPS (upgrades)
                        // or HTTPS to HTTPS
                        if (shouldFollowRedirect(currentUri, newUri)) {
                            log.debug("Following redirect from ${currentUrl} to ${newUrl}")
                            httpConn.disconnect()
                            currentUrl = newUrl
                            currentUri = newUri

                            redirectCount++
                            continue
                        } else {
                            httpConn.disconnect()
                            throw new IOException("Unsafe redirect from ${currentUrl} to ${newUrl}")
                        }
                    }
                }

                // Not a redirect, return the connection
                return httpConn
            }

            // For non-HTTP connections, just return
            return conn
        }

        throw new IOException("Too many redirects (${maxHttpRedirectFollows}) for URL: ${originalUrl}")
    }

    /**
     * Determine if a redirect should be followed based on security considerations
     */
    boolean shouldFollowRedirect(URI from, URI to) {
        String fromProto = from.scheme?.toLowerCase()
        String toProto = to.scheme?.toLowerCase()

        // Allow HTTP to HTTPS (upgrade)
        if (fromProto == 'http' && toProto == 'https') {
            return true
        }

        // Allow HTTPS to HTTPS
        if (fromProto == 'https' && toProto == 'https') {
            return true
        }

        // Allow HTTPS to HTTP (downgrade) only if:
        // 1. No userinfo in either URL
        // 2. No query parameters that might contain sensitive data
        // 3. Same host (to prevent leaking to third parties)
        if (fromProto == 'https' && toProto == 'http') {
            if (from.userInfo || to.userInfo) {
                log.debug("Rejecting HTTPS to HTTP redirect due to userinfo")
                return false
            }

            // Check for potentially sensitive query parameters using configurable list
            if (from.query && hasSensitiveParams(from.query)) {
                log.debug("Rejecting HTTPS to HTTP redirect due to sensitive query parameters")
                return false
            }

            // Only allow downgrade to same host
            if (from.host?.toLowerCase() != to.host?.toLowerCase()) {
                log.debug("Rejecting HTTPS to HTTP redirect to different host")
                return false
            }

            log.warn("Following HTTPS to HTTP downgrade for same host: ${from.host}")
            return true
        }

        // Allow HTTP to HTTP with security checks
        if (fromProto == 'http' && toProto == 'http') {
            // All the following checks default to false to match the default behaviour
            // of HttpUrlConnection
            // If different-host restriction is enabled, check for it
            if (disallowHttpToHttpDifferentHost) {
                if (from.host?.toLowerCase() != to.host?.toLowerCase()) {
                    log.debug("Rejecting HTTP to HTTP redirect to different host (disallowHttpToHttpDifferentHost=true)")
                    return false
                }
            }

            // Check for userinfo (credentials) - never redirect with credentials
            if (disallowHttpToHttpUserInfo) {
                if (from.userInfo || to.userInfo) {
                    log.debug("Rejecting HTTP to HTTP redirect due to userinfo")
                    return false
                }
            }

            // Check for sensitive parameters
            if (disallowHttpToHttpSensitiveParams) {
                if (from.query && hasSensitiveParams(from.query)) {
                    log.debug("Rejecting HTTP to HTTP redirect due to sensitive query parameters")
                    return false
                }
            }

            return true
        }

        // Reject all other protocol changes
        return false
    }

    /**
     * URL overload for backward compatibility
     */
    boolean shouldFollowRedirect(URL from, URL to) {
        return shouldFollowRedirect(from.toURI(), to.toURI())
    }

    /**
     * Check if query string contains any sensitive parameters
     */
    boolean hasSensitiveParams(String query) {
        if (!query) return false

        def queryParams = query.toLowerCase().split('&').collect {
            def parts = it.split('=')
            parts.length > 0 ? parts[0] : ''
        }

        return queryParams.any { param ->
            sensitiveParams?.any { sensitive -> param.contains(sensitive.toLowerCase()) }
        }
    }

    /**
     * Create a URLConnection with configured timeouts and user agent
     */
    private URLConnection createConnection(URL url) {
        def conn = url.openConnection()
        conn.connectTimeout = connectTimeoutMs
        conn.readTimeout = readTimeoutMs
        conn.setRequestProperty(USER_AGENT, getUserAgent())
        return conn
    }

    /**
     * Check if a URL is accessible by making a HEAD request.
     * Returns true if the URL returns a 2xx or 3xx status code, false otherwise.
     */
    @NotTransactional
    boolean checkUrlAccessible(String urlString) {
        try {
            if (useHttpClient) {
                URI uri = new URI(urlString)
                if (uri.scheme == 'http' || uri.scheme == 'https') {
                    def response = createHttpResponseWithHttpClient(uri, urlString, 'HEAD', Void)
                    int statusCode = response.statusCode()
                    return statusCode >= 200 && statusCode < 400
                } else if (uri.scheme == 's3') {
                    return new S3HttpResponse(uri).getStatusCode() == 200
                }
            }
            // Fall back to URLConnection
            def url = new URL(urlString)
            def conn = createConnectionWithRedirects(url, urlString)

            if (conn instanceof HttpURLConnection) {
                HttpURLConnection httpConn = (HttpURLConnection) conn
                httpConn.requestMethod = 'HEAD'

                try {
                    int statusCode = httpConn.getResponseCode()
                    return statusCode >= 200 && statusCode < 400
                } finally {
                    httpConn.disconnect()
                }
            }

            // For non-HTTP connections, just return true if we could connect
            return true
        } catch (Exception e) {
            log.debug("URL not accessible: ${urlString} - ${e.message}")
            return false
        }
    }

    /**
     * Log a failed URL attempt to the database
     */
    @Transactional
    def logBadUrl(String url, Integer statusCode = null, String errorMessage = null){
        new FailedUpload(
            url: url,
            httpStatusCode: statusCode,
            errorMessage: errorMessage
        ).save(flush: true, failOnError: false)
    }

    /**
     * Check if a URL has previously failed
     */
    boolean isBadUrl(String url){
        FailedUpload.exists(url)//countByUrl(url) > 0
    }

    /**
     * Detect MIME type from byte array
     */
    static String detectMimeTypeFromBytes(byte[] bytes, String filename) {
        return new MetadataExtractor().detectContentType(bytes, filename)
    }

    /**
     * Detect MIME type from ByteSource
     */
    static String detectMimeType(ByteSource byteSource, String filename) {
        return new MetadataExtractor().detectContentType(byteSource, filename)
    }

    /**
     * Get the user agent string to use for HTTP requests
     */
    private String getUserAgent() {
        def ua = this.userAgent
        if (!ua) {
            ua = "$orgNameShort-$appName/$version"
        }
        return ua
    }
}

