package au.org.ala.images.metrics

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer

import java.util.concurrent.TimeUnit

/**
 * InputStream wrapper that tracks read performance metrics including:
 * - Time until stream is closed
 * - Total bytes read
 * - Throughput (bytes/sec)
 */
@CompileStatic
@Slf4j
class MetricsTrackingInputStream extends FilterInputStream {

    private final MeterRegistry meterRegistry
    private final String bucket
    private final String region
    private final long startTimeNanos
    private long bytesRead = 0
    private boolean closed = false

    MetricsTrackingInputStream(InputStream delegate, MeterRegistry meterRegistry, String bucket, String region) {
        super(delegate)
        this.meterRegistry = meterRegistry
        this.bucket = bucket
        this.region = region
        this.startTimeNanos = System.nanoTime()
    }

    @Override
    int read() throws IOException {
        int b = super.read()
        if (b != -1) {
            bytesRead++
        }
        return b
    }

    @Override
    int read(byte[] b) throws IOException {
        int n = super.read(b)
        if (n > 0) {
            bytesRead += n
        }
        return n
    }

    @Override
    int read(byte[] b, int off, int len) throws IOException {
        int n = super.read(b, off, len)
        if (n > 0) {
            bytesRead += n
        }
        return n
    }

    @Override
    void close() throws IOException {
        if (!closed) {
            closed = true
            long durationNanos = System.nanoTime() - startTimeNanos
            double durationSeconds = durationNanos / 1_000_000_000.0

            // Record the stream lifetime duration
            if (meterRegistry != null) {
                Timer.builder("s3.stream.duration")
                    .tag("bucket", bucket ?: "unknown")
                    .tag("region", region ?: "unknown")
                    .description("Time from stream open to close")
                    .register(meterRegistry)
                    .record(durationNanos, TimeUnit.NANOSECONDS)

                // Record total bytes read
                meterRegistry.counter("s3.stream.bytes",
                    "bucket", bucket ?: "unknown",
                    "region", region ?: "unknown")
                    .increment(bytesRead)

                // Calculate and record throughput (bytes/sec)
                if (durationSeconds > 0) {
                    double throughput = bytesRead / durationSeconds
                    meterRegistry.gauge("s3.stream.throughput.bps",
                        List.of(
                            Tag.of("bucket", bucket ?: "unknown"),
                            Tag.of("region", region ?: "unknown")
                        ),
                        throughput)

                    // Also record throughput in MB/s for easier reading
                    double throughputMBps = throughput / (1024 * 1024)
                    meterRegistry.gauge("s3.stream.throughput.mbps",
                        List.of(
                            Tag.of("bucket", bucket ?: "unknown"),
                            Tag.of("region", region ?: "unknown")
                        ),
                        throughputMBps)
                }

                if (log.isTraceEnabled()) {
                    log.trace("S3 stream closed: bucket={}, region={}, bytes={}, duration={}s, throughput={} MB/s",
                        bucket, region, bytesRead, String.format("%.3f", durationSeconds),
                        bytesRead > 0 && durationSeconds > 0 ? String.format("%.2f", (bytesRead / durationSeconds) / (1024 * 1024)) : "N/A")
                }
            }

            super.close()
        }
    }
}

