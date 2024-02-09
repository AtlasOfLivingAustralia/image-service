package au.org.ala.images

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.services.analytics.AnalyticsScopes
import groovy.json.JsonSlurper

import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

class AnalyticsService {

    def collectoryService
    def grailsApplication

    final analyticsExecutor = Executors.newSingleThreadExecutor()

    def REPORT_PERIODS = [
        "thisMonth": "30daysAgo",
        "last3Months": "90daysAgo",
        "lastYear": "365daysAgo"
    ]

    Object byDataResource(String dataResourceUID) {

        def results = [:]

        if (dataResourceUID && getAccessToken()){

            def now = (new Date() + 1 ).format( 'yyyy-MM-dd' )
            //do authentication....
            def googleApiBaseUrl = grailsApplication.config.getProperty('analytics.baseURL')
            def googleViewID = URLEncoder.encode(grailsApplication.config.getProperty('analytics.viewID'), "UTF-8")

            REPORT_PERIODS.each { label, period ->
                def lastMonth = "${googleApiBaseUrl}?ids=${googleViewID}&start-date=30daysAgo&end-date=${now}&dimensions=ga%3AeventCategory&metrics=ga%3AuniqueEvents&filters=ga%3AeventAction%3D%3D${dataResourceUID}&access_token=${accessToken}"
                def js = new JsonSlurper()
                def rows = js.parse(new URL(lastMonth)).rows
                def totalEvents = 0
                rows.each {k, v -> totalEvents+= v as Integer}
                results[label] = ["totalEvents": totalEvents, "events": js.parse(new URL(lastMonth)).rows]
            }
        }
        results
    }

    Object byAll() {

        def results = [:]

        if (getAccessToken()) {
            def now = (new Date() + 1).format('yyyy-MM-dd')

            def googleApiBaseUrl = grailsApplication.config.getProperty('analytics.baseURL')
            def googleViewID = URLEncoder.encode(grailsApplication.config.getProperty('analytics.viewID'), "UTF-8")

            REPORT_PERIODS.each { label, period ->
                def lastMonth = "${googleApiBaseUrl}?ids=${googleViewID}&start-date=${period}&end-date=${now}&dimensions=ga%3AeventAction&metrics=ga%3AuniqueEvents&&access_token=${getAccessToken()}"
                def js = new JsonSlurper()
                def totalEvents = 0
                def rows = js.parse(new URL(lastMonth)).rows
                def enhanced = []
                rows.each { key, value ->
                    enhanced << ["uid": key, "name": collectoryService.getNameForUID(key), "count": value]
                }

                enhanced.sort {}

                rows.each { k, v -> totalEvents += v as Integer }
                results[label] = [
                        "totalEvents": totalEvents,
                        "entities"   : enhanced
                ]
            }
        }
        results
    }

    String getAccessToken(){
        def credentialFile = new File(grailsApplication.config.getProperty('analytics.credentialsJson'))
        if (credentialFile.exists()) {
            GoogleCredential credential = GoogleCredential
                    .fromStream(new FileInputStream(grailsApplication.config.getProperty('analytics.credentialsJson')))
                    .createScoped(Collections.singleton(AnalyticsScopes.ANALYTICS_READONLY));
            credential.refreshToken()
            return credential.getAccessToken()
        } else {
            null
        }
    }

    /**
     * POST event data to google analytics.
     *
     * @param imageInstance
     * @param eventCategory
     * @return
     */
    def sendAnalytics(Image imageInstance, String eventCategory, String userAgent) {
        final analyticsId = grailsApplication.config.getProperty('analytics.ID')
        if (imageInstance && analyticsId) {
            final queryURL =  grailsApplication.config.getProperty('analytics.URL')
            final requestBody = [
                    'v': 1,
                    'tid': analyticsId,
                    'cid': UUID.randomUUID().toString(),  //anonymous client ID
                    't': 'event',
                    'ec': eventCategory, // event category
                    'ea': imageInstance.dataResourceUid, //event value
                    'ua' : userAgent
            ]

            analyticsExecutor.execute {
                try {
                    HttpURLConnection connection = (HttpURLConnection) queryURL.toString().toURL().openConnection()
                    connection.requestMethod = 'POST'
                    connection.setRequestProperty('Content-Type', 'application/x-www-form-urlencoded')
                    def formBody = toFormBody(requestBody).getBytes(StandardCharsets.UTF_8)
                    connection.doOutput = true
                    connection.outputStream.withStream {
                        it.write(formBody)
                    }
                    def responseCode = connection.responseCode
                    if (responseCode < 300) {
                        log.debug("Analytics POST response status: {}", connection.responseMessage)
                    } else {
                        log.error("analytics request failed = {}", connection.responseMessage)
                    }
                } catch (Exception e) {
                    log.error('Unable to send analytics for {}', requestBody, e)
                }
            }
        }
    }

    private String toFormBody(Map<String, Serializable> form) {
        form.collect { k,v -> URLEncoder.encode(k, "UTF-8")+'='+URLEncoder.encode(v, "UTF-8") }.join('&')
    }
}
