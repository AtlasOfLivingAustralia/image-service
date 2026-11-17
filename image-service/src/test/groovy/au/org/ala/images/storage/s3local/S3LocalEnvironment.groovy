package au.org.ala.images.storage.s3local

import groovy.json.JsonOutput
import groovy.util.logging.Slf4j
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.NoSuchBucketException

import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Local S3 (Versity gateway) behind Toxiproxy, either started with Testcontainers or attached to the
 * already-running s3-local/docker-compose.yml stack when {@code -Ds3local.external=true}.
 * Ports and credentials match the compose file so both modes behave identically.
 */
@Slf4j
class S3LocalEnvironment implements AutoCloseable {

    static final String ACCESS_KEY = 'localdev'
    static final String SECRET_KEY = 'localdev-secret'
    static final String REGION = 'us-east-1'
    static final String PROXY_NAME = 's3'
    static final int S3_PORT = 7070
    static final int PROXY_PORT = 8666
    static final int TOXIPROXY_API_PORT = 8474

    final boolean external
    final String directEndpoint
    final String proxiedEndpoint
    final String toxiproxyApi

    private Network network
    private GenericContainer s3Container
    private GenericContainer toxiproxyContainer
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()

    private S3LocalEnvironment(boolean external, String directEndpoint, String proxiedEndpoint, String toxiproxyApi,
                               Network network, GenericContainer s3Container, GenericContainer toxiproxyContainer) {
        this.external = external
        this.directEndpoint = directEndpoint
        this.proxiedEndpoint = proxiedEndpoint
        this.toxiproxyApi = toxiproxyApi
        this.network = network
        this.s3Container = s3Container
        this.toxiproxyContainer = toxiproxyContainer
    }

    static S3LocalEnvironment start() {
        if (Boolean.getBoolean('s3local.external')) {
            String host = System.getProperty('s3local.host', 'localhost')
            log.info('Using external s3-local compose stack on {}', host)
            def env = new S3LocalEnvironment(true, "http://$host:$S3_PORT", "http://$host:$PROXY_PORT",
                    "http://$host:$TOXIPROXY_API_PORT", null, null, null)
            env.reset()
            return env
        }

        Network network = Network.newNetwork()
        GenericContainer s3 = new GenericContainer(DockerImageName.parse('versity/versitygw:v1.10.0'))
                .withNetwork(network)
                .withNetworkAliases('s3')
                .withCommand('--access', ACCESS_KEY, '--secret', SECRET_KEY, '--port', ":$S3_PORT", 'posix', '/data')
                .withTmpFs(['/data': 'rw'])
                .withExposedPorts(S3_PORT)
                .waitingFor(Wait.forListeningPort())
        s3.start()

        GenericContainer toxiproxy = new GenericContainer(DockerImageName.parse('ghcr.io/shopify/toxiproxy:2.12.0'))
                .withNetwork(network)
                .withCommand('-host=0.0.0.0')
                .withExposedPorts(TOXIPROXY_API_PORT, PROXY_PORT)
                .waitingFor(Wait.forHttp('/version').forPort(TOXIPROXY_API_PORT))
        toxiproxy.start()

        def env = new S3LocalEnvironment(false,
                "http://${s3.host}:${s3.getMappedPort(S3_PORT)}",
                "http://${toxiproxy.host}:${toxiproxy.getMappedPort(PROXY_PORT)}",
                "http://${toxiproxy.host}:${toxiproxy.getMappedPort(TOXIPROXY_API_PORT)}",
                network, s3, toxiproxy)
        env.post('/proxies', [name: PROXY_NAME, listen: "0.0.0.0:$PROXY_PORT", upstream: "s3:$S3_PORT", enabled: true])
        log.info('Started s3-local containers: direct={} proxied={} toxiproxy={}', env.directEndpoint, env.proxiedEndpoint, env.toxiproxyApi)
        return env
    }

    /** Plain sync client straight to S3, bypassing toxiproxy, for setup and verification. */
    S3Client directClient() {
        S3Client.builder()
                .endpointOverride(URI.create(directEndpoint))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
                .region(Region.of(REGION))
                .forcePathStyle(true)
                .build()
    }

    void createBucket(String bucket) {
        directClient().withCloseable { S3Client client ->
            try {
                client.headBucket { it.bucket(bucket) }
            } catch (NoSuchBucketException ignored) {
                client.createBucket { it.bucket(bucket) }
            }
        }
    }

    // ---- toxiproxy controls (same semantics as s3-local/toxic.sh) ----

    /** Removes all toxics and re-enables the proxy. */
    void reset() { post('/reset', [:]) }

    void addToxic(String name, String type, String stream, Map attributes) {
        post("/proxies/$PROXY_NAME/toxics", [name: name, type: type, stream: stream, toxicity: 1.0, attributes: attributes])
        log.info('toxiproxy: added {} ({}, {}) {}', name, type, stream, attributes)
    }

    void removeToxic(String name) {
        send(HttpRequest.newBuilder(URI.create("$toxiproxyApi/proxies/$PROXY_NAME/toxics/$name")).DELETE().build())
        log.info('toxiproxy: removed {}', name)
    }

    /** Accept connections but never forward data: models half-open / silently dropped (NAT idle) connections. */
    void blackhole(String stream = 'upstream') { addToxic("blackhole_$stream", 'timeout', stream, [timeout: 0]) }

    void resetPeer(int afterMillis = 0) { addToxic('reset_peer', 'reset_peer', 'upstream', [timeout: afterMillis]) }

    void latency(int millis) {
        addToxic('latency_up', 'latency', 'upstream', [latency: millis])
        addToxic('latency_down', 'latency', 'downstream', [latency: millis])
    }

    /** Disabling the proxy drops all current connections and refuses new ones. */
    void proxyEnabled(boolean enabled) {
        post("/proxies/$PROXY_NAME", [enabled: enabled])
        log.info('toxiproxy: proxy enabled={}', enabled)
    }

    private void post(String path, Map body) {
        send(HttpRequest.newBuilder(URI.create("$toxiproxyApi$path"))
                .header('Content-Type', 'application/json')
                .POST(HttpRequest.BodyPublishers.ofString(JsonOutput.toJson(body)))
                .build())
    }

    private void send(HttpRequest request) {
        def response = http.send(request, HttpResponse.BodyHandlers.ofString())
        if (response.statusCode() >= 300) {
            throw new IllegalStateException("toxiproxy ${request.method()} ${request.uri()} -> ${response.statusCode()}: ${response.body()}")
        }
    }

    @Override
    void close() {
        if (external) {
            reset()
            return
        }
        toxiproxyContainer?.stop()
        s3Container?.stop()
        network?.close()
    }
}
