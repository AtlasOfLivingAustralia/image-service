package au.org.ala.images.metrics

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer

import java.util.concurrent.TimeUnit

/**
 * OutputStream wrapper that tracks write performance metrics including:
 * - Time until stream is closed
 * - Total bytes written
 * - Throughput (bytes/sec)
 * - Operation type (streaming vs buffered)
 */
@CompileStatic
@Slf4j
class MetricsTrackingOutputStream extends FilterOutputStream {
    
    private final MeterRegistry meterRegistry
    private final String bucket
    private final String region
    private final String operationType  // "streaming" or "buffered"
    private final long startTimeNanos
    private long bytesWritten = 0
    private boolean closed = false
    
    MetricsTrackingOutputStream(OutputStream delegate, MeterRegistry meterRegistry, String bucket, String region, String operationType) {
        super(delegate)
        this.meterRegistry = meterRegistry
        this.bucket = bucket ?: "unknown"
        this.region = region ?: "unknown"
        this.operationType = operationType
        this.startTimeNanos = System.nanoTime()
    }
    
    @Override
    void write(int b) throws IOException {
        super.write(b)
        bytesWritten++
    }
    
    @Override
    void write(byte[] b) throws IOException {
        super.write(b)
        bytesWritten += b.length
    }
    
    @Override
    void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len)
        bytesWritten += len
    }
    
    @Override
    void close() throws IOException {
        if (!closed) {
            closed = true
            long durationNanos = System.nanoTime() - startTimeNanos
            double durationSeconds = durationNanos / 1_000_000_000.0
            
            // Record metrics before closing the underlying stream
            recordMetrics(durationNanos, durationSeconds)
            
            try {
                super.close()
            } catch (IOException e) {
                // Record error metric
                if (meterRegistry != null) {
                    meterRegistry.counter("s3.upload.error",
                        "bucket", bucket,
                        "region", region,
                        "type", operationType,
                        "error", e.class.simpleName)
                        .increment()
                }
                throw e
            }
        }
    }
    
    private void recordMetrics(long durationNanos, double durationSeconds) {
        if (meterRegistry == null) {
            return
        }
        
        try {
            // Record the upload duration
            Timer.builder("s3.upload.duration")
                .tag("bucket", bucket)
                .tag("region", region)
                .tag("type", operationType)
                .description("Time from stream open to close for S3 uploads")
                .register(meterRegistry)
                .record(durationNanos, TimeUnit.NANOSECONDS)
            
            // Record total bytes written
            meterRegistry.counter("s3.upload.bytes",
                "bucket", bucket,
                "region", region,
                "type", operationType)
                .increment(bytesWritten)
            
            // Record success counter
            meterRegistry.counter("s3.upload.success",
                "bucket", bucket,
                "region", region,
                "type", operationType)
                .increment()
            
            // Calculate and record throughput (bytes/sec)
            if (durationSeconds > 0 && bytesWritten > 0) {
                double throughput = bytesWritten / durationSeconds
                meterRegistry.gauge("s3.upload.throughput.bps",
                    List.of(
                        Tag.of("bucket", bucket),
                        Tag.of("region", region),
                        Tag.of("type", operationType)
                    ),
                    throughput)
                
                // Also record throughput in MB/s for easier reading
                double throughputMBps = throughput / (1024 * 1024)
                meterRegistry.gauge("s3.upload.throughput.mbps",
                    List.of(
                        Tag.of("bucket", bucket),
                        Tag.of("region", region),
                        Tag.of("type", operationType)
                    ),
                    throughputMBps)
            }
            
            if (log.isTraceEnabled()) {
                log.trace("S3 upload stream closed: bucket={}, region={}, type={}, bytes={}, duration={}s, throughput={} MB/s",
                    bucket, region, operationType, bytesWritten, String.format("%.3f", durationSeconds),
                    bytesWritten > 0 && durationSeconds > 0 ? String.format("%.2f", (bytesWritten / durationSeconds) / (1024 * 1024)) : "N/A")
            }
        } catch (Exception e) {
            log.warn("Failed to record S3 upload metrics", e)
        }
    }
}

