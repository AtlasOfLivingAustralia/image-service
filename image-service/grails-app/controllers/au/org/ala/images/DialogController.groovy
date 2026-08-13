package au.org.ala.images

import grails.web.servlet.mvc.GrailsParameterMap

class DialogController {

    def imageService

    def areYouSureFragment() {
        def message = params.message
        def affirmativeText = params.affirmativeText ?: "Yes"
        def negativeText = params.negativeText ?: "No"

        [message: message, affirmativeText: affirmativeText, negativeText: negativeText]
    }

    def addUserMetadataFragment() {
    }

    def pleaseWaitFragment() {
        [message: params.message ?: "Please wait..."]
    }

    def calibrateImageFragment() {
        def image = imageService.getImageFromParams(params)
        def pixelLength = params.pixelLength
        [imageInstance: image, pixelLength: pixelLength]
    }

    def selectImagesForStagingFragment() {
        [userId: params.userId]
    }

    def uploadStagedImagesDataFileFragment() {
        [userId: params.userId]
    }

}
