package au.org.ala.images

import grails.gorm.async.AsyncEntity

class AuditMessage implements AsyncEntity<AuditMessage> {

    String imageIdentifier
    String message
    String userId
    Date dateCreated

    static constraints = {
        imageIdentifier nullable: false
        message nullable: false, maxSize: 2048
        userId nullable: false
        dateCreated nullable: true
    }

}
