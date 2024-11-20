package au.org.ala.images

import javax.annotation.PostConstruct
import javax.annotation.PreDestroy

class AuditService {

    static AuditService AUDIT_SERVICE

    @PostConstruct
    void init() {
        AUDIT_SERVICE = this
    }

    @PreDestroy
    void destroy() {
        AUDIT_SERVICE = null
    }

    def log(Image image, String message, String userId) {
        def auditMessage = new AuditMessage(imageIdentifier: image.imageIdentifier, message: message, userId: userId)
        AuditMessage.async.saveAll(auditMessage)
    }

    def log(String imageIdentifier, String message, String userId) {
        def auditMessage = new AuditMessage(imageIdentifier: imageIdentifier, message: message, userId: userId)
        AuditMessage.async.saveAll(auditMessage)
    }

    // allow domain objects to submit audit logs without forcing them to opt in to autowiring
    static void submitLog(String imageIdentifier, String message, String userId) {
        AUDIT_SERVICE?.log(imageIdentifier, message, userId)
    }

    def getMessagesForImage(String imageIdentifier) {
        return AuditMessage.findAllByImageIdentifier(imageIdentifier, [sort:'dateCreated', order:'asc'])
    }

}
