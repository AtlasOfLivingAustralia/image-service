package au.org.ala.images.storage

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory
import software.amazon.awssdk.core.exception.ApiCallAttemptTimeoutException
import spock.lang.Specification

class S3UploadProgressSpec extends Specification {

    ListAppender<ILoggingEvent> appender = new ListAppender<>()
    Logger logger = (Logger) LoggerFactory.getLogger(S3UploadProgress)

    def setup() {
        appender.start()
        logger.addAppender(appender)
    }

    def cleanup() {
        logger.detachAppender(appender)
    }

    def 'timeout while the producer is still writing is called out as not stuck'() {
        given:
        def progress = new S3UploadProgress('streaming', 'b', 'k')
        progress.progressed(100)

        when:
        progress.reportFailure(new IOException(ApiCallAttemptTimeoutException.create(5000)))

        then:
        appender.list.size() == 1
        appender.list[0].level.toString() == 'WARN'
        appender.list[0].formattedMessage.contains('STILL WRITING')
        appender.list[0].formattedMessage.contains('bytesWritten=100')
    }

    def 'timeout with no recent progress is reported as consistent with a stuck connection'() {
        given:
        def progress = new S3UploadProgress('ingest', 'b', 'k')
        Thread.sleep(S3UploadProgress.ACTIVE_WINDOW_MS + 100)

        when:
        progress.reportFailure(new java.util.concurrent.TimeoutException())

        then:
        appender.list[0].formattedMessage.contains('stuck connection')
    }

    def 'non-timeout failures are reported once'() {
        given:
        def progress = new S3UploadProgress('streaming', 'b', 'k')

        when:
        progress.reportFailure(new IOException('boom'))
        progress.reportFailure(new IOException('again'))

        then:
        appender.list.size() == 1
        appender.list[0].formattedMessage.contains('upload failed')
        !S3UploadProgress.isTimeout(new IOException('boom'))
    }
}
