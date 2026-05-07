package au.org.ala.images

class TagController {

    def index() {}

    def createTagFragment() {
        def parentTag = Tag.get(params.int("parentTagID"))
        [parentTag: parentTag]
    }

    def renameTagFragment() {
        def tag = Tag.get(params.int("tagID"))
        [tagInstance: tag]
    }

    def deleteTagFragment() {
        def tag = Tag.get(params.int("tagID"))
        [tagInstance: tag]
    }

    def selectTagFragment() {
        [:]
    }
}
