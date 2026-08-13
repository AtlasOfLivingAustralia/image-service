package au.org.ala.images

import grails.gorm.transactions.Transactional
import groovy.util.logging.Slf4j
import org.springframework.web.multipart.MultipartFile

@Transactional
@Slf4j
class TagService {

    def auditService

    def createTagByPath(String path, Tag parent = null) {

        if (path.startsWith(TagConstants.TAG_PATH_SEPARATOR)) {
            path = path.substring(TagConstants.TAG_PATH_SEPARATOR.length())
        }

        path = path.toLowerCase()

        def bits = path.split(TagConstants.TAG_PATH_SEPARATOR)
        def partial = []

        if (parent) {
            def parentBits = parent.path.split(TagConstants.TAG_PATH_SEPARATOR)
            parentBits.each {
                if (it) {
                    partial << it
                }
            }
        }

        Tag tag
        bits.each { pathElement ->
            partial << pathElement
            def intermediatePath = TagConstants.TAG_PATH_SEPARATOR + partial.join(TagConstants.TAG_PATH_SEPARATOR)
            tag = Tag.findByPath(intermediatePath)
            if (!tag) {
                tag = new Tag(path: intermediatePath)
                tag.save()
            }
        }

        tag
    }

    def moveTag(Tag target, Tag newParent) {

        // First identify all the child tags of the target, because they need to move too
        if (!target) {
            return false
        }

        if (target == newParent) {
            return false
        }

        log.info("Moving ${target.path} to ${newParent?.path ?: " to root"}")

        def newParentPath = newParent?.path ?: ""

        def tagList = [target]
        def children = findChildTagsByPath(target.path)
        tagList.addAll(children)

        // Now update all the paths...
        def oldPrefix = target.path.substring(0, target.path.lastIndexOf(TagConstants.TAG_PATH_SEPARATOR))
        tagList.each { tag ->
            def path = tag.path
            def newPath = newParentPath + (path - oldPrefix)
            tag.path = newPath
        }
    }

    private List<Tag> findChildTagsByPath(String parentPath) {
        if (!parentPath.endsWith(TagConstants.TAG_PATH_SEPARATOR)) {
            parentPath += TagConstants.TAG_PATH_SEPARATOR
        }

        parentPath = parentPath.replaceAll('\\_', "\\\\_")
        def c = Tag.createCriteria()
        return c {
            sqlRestriction("path ilike '${parentPath}%' ESCAPE '\\'")
        }
    }

    private List<Tag> findParentTagsByPath(String path) {

        if (path.startsWith(TagConstants.TAG_PATH_SEPARATOR)) {
            path = path.substring(TagConstants.TAG_PATH_SEPARATOR.length())
        }

        def partial = []
        def bits = path.split(TagConstants.TAG_PATH_SEPARATOR)
        def tags = []
        bits.each { pathElement ->
            partial << pathElement
            def partialPath = TagConstants.TAG_PATH_SEPARATOR + partial.join(TagConstants.TAG_PATH_SEPARATOR)
            def tag = Tag.findByPath(partialPath)
            if (tag) {
                tags << tag
            }
        }
        return tags.sort { Tag tag -> 1.0 / ((double) tag.path.length()) }
    }

    def renameTag(Tag tag, String newSuffix) {
        if (!tag || !newSuffix) {
            return
        }

        def pathPrefix = tag.path.substring(0, tag.path.lastIndexOf(TagConstants.TAG_PATH_SEPARATOR))
        def newPath = pathPrefix + TagConstants.TAG_PATH_SEPARATOR + newSuffix.toLowerCase()
        def children = findChildTagsByPath(tag.path)

        children.each { child ->
            def suffix = child.path - tag.path
            def newChildPath = newPath + suffix
            child.path = newChildPath
        }
        tag.path = newPath
    }

    def deleteTag(Tag tag) {
        def purgelist = [tag]
        def children = findChildTagsByPath(tag.path)
        purgelist.addAll(children)

        purgelist.each { t ->
            // First remove the tag from any images
            def imageTags = ImageTag.findAllByTag(t)
            imageTags.each { imageTag ->
                imageTag.delete()
            }
            t.delete()
        }
    }

    def updateTags(Image image, List tags, String userId){
        if (tags && image) {
            tags.each { String tagPath ->
                def tag = createTagByPath(tagPath)
                attachTagToImage(image, tag, userId)
            }
        }
    }

    def attachTagToImage(Image image, Tag tag, String userId) {

        if (!image || !tag) {
            return false
        }

        boolean addedAtLeastOneTag = false
        def alreadyTagged = false
        def allTags = [tag]
        def parents = findParentTagsByPath(tag.path)
        allTags.addAll(parents)

        allTags.each { t ->
            // need to see if this image already has this tag to avoid duplicates
            def taggedImage = ImageTag.findByImageAndTag(image, t)
            if (!taggedImage) {
                // can create it
                taggedImage = new ImageTag(image: image, tag: t)
                taggedImage.save()
                addedAtLeastOneTag = true
            }
        }
        rebuildKeywords(image)
        auditService.log(image, "Tag attached to image: ${tag.path}", userId)
        addedAtLeastOneTag
    }

    def detachTagFromImage(Image image, Tag tag) {
        boolean detachedAtLeastOneTag = false
        if (!image || !tag) {
            return false
        }

        def allTags = ImageTag.findAllByImage(image)?.collect { it.tag.path }

        def parents = findParentTagsByPath(tag.path)

        // kill this tag explicitly...
        def taggedImage = ImageTag.findByImageAndTag(image, tag)
        if (taggedImage) {
            taggedImage.delete()
            allTags.remove taggedImage.tag.path
        }

        // For each parent, only delete the parent if it has no other children
        for (Tag t : parents) {
        def childPathPrefix = t.path + TagConstants.TAG_PATH_SEPARATOR
            def existing = allTags.find { it.startsWith(childPathPrefix) }
            if (existing) {
                break;
            }

            // otherwise delete the parent too
            taggedImage = ImageTag.findByImageAndTag(image, t)
            if (taggedImage) {
                taggedImage.delete()
                detachedAtLeastOneTag = true
                allTags.remove(taggedImage.tag.path)
            }
        }

        rebuildKeywords(image)

        return detachedAtLeastOneTag
    }

    def rebuildKeywords(Image image) {
        if (!image) {
            return
        }

        log.info("Rebuilding keyword for image ${image.imageIdentifier}")

        // remove all existing keywords
        image.keywords?.each { imageKeyword ->
            imageKeyword.delete()
        }
        image.keywords?.clear()

        def tagPaths = ImageTag.findAllByImage(image)?.collect { it.tag.path }
        Set imageKeywords = new HashSet<String>()
        tagPaths.each { tagPath ->
            def keywords = tagPath.split('/')
            keywords.each { keyword ->
                if (keyword) {
                    imageKeywords.add(keyword)
                }
            }
        }

        imageKeywords.each { keyword ->
            image.addToKeywords(new ImageKeyword(image: image, keyword: keyword))
        }
    }

    def loadTagsFromFile(MultipartFile file) {
        file.inputStream?.eachCsvLine {
            def line = it.join(",")
            createTagByPath(line, null)
        }
        return 0
    }

}
