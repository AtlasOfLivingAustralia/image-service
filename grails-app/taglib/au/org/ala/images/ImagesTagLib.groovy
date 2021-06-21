package au.org.ala.images

import groovy.xml.MarkupBuilder
import org.apache.commons.lang3.StringUtils

class ImagesTagLib {

    static namespace = 'img'

    static returnObjectForTags = ['sanitiseString']

    def imageService
    def groovyPageLocator
    def authService
    def searchCriteriaService
    def collectoryService
    def sanitiserService

    /**
     * @attr title
     * @attr selectedNavItem
     * @attr crumbLabel
     * @attr hideTitle
     * @attr hideCrumbs
     */
    def headerContent = { attrs, body ->

        def mb = new MarkupBuilder(out)
        def bodyContent = body.call()
        def crumbLabel = attrs.crumbLabel ?: attrs.title

        sitemesh.captureContent(tag:'page-header') {

            def crumbList = []
            def keyIndex = 1

            if (pageScope.crumbs) {
                crumbList = pageScope.crumbs
            } else {
                Map crumb
                while (crumb = attrs.getAt("breadcrumb${keyIndex++}")) {
                    crumbList << crumb
                }
            }

            if (!attrs.hideCrumbs) {
                mb.nav(id:'breadcrumb') {
                    ol {
                        li {
                            a(href:createLink(uri:'/')) {
                                mkp.yield("Home")
                            }
                            span(class:"icon icon-arrow-right")
                        }
                        if (crumbList) {
                            for (int i = 0; i < crumbList?.size(); i++) {
                                def item = crumbList[i]
                                li {
                                    a(href: item.link) {
                                        mkp.yield(item.label)
                                    }
                                    span(class:"icon icon-arrow-right")
                                }
                            }
                        }
                        li(class:'last') {
                            span {
                                mkp.yield(crumbLabel)
                            }
                        }
                    }
                }
            }

            if (!attrs.hideTitle) {
                mb.h2 {
                    mkp.yield(attrs.title)
                }
            }

            if (bodyContent) {
                mb.div {
                    mb.mkp.yieldUnescaped(bodyContent)
                }
            }
        }
    }

    def spinner = { attrs, body ->
        if (attrs.dark) {
            out << "<image src=\"${resource(dir:'images', file:'spinner-dark.gif')}\" />"
        } else {
            out << "<image src=\"${resource(dir:'images', file:'spinner-transparent.gif')}\" />"
        }
    }

    /**
     * @attr imageId
     */
    def imageUrl = { attrs, body ->
        if (attrs.imageId) {
            out << imageService.getImageUrl(attrs.imageId as String)
        }
    }

    def imageThumbUrl = { attrs, body ->
        if (attrs.imageId ) {
            out << imageService.getImageThumbUrl(attrs.imageId as String)
        }
    }

    def imageSearchResult = { attrs, body ->

        def mb = new MarkupBuilder(out)

        if (attrs.image) {
            def creator = ''
            if (attrs.image.creator && attrs.image.creator != ElasticSearchService.NOT_SUPPLIED){
                creator = attrs.image.creator
            }

            if(attrs.image.dataResourceUid){
                def metadata = collectoryService.getResourceLevelMetadata(attrs.image.dataResourceUid)
                mb.div(class: ['thumb-caption', 'caption-detail', attrs.css ?: ''].findAll().join(' ')) {
                    mb.span(class: 'resource-name') {
                        mkp.yield(metadata.name ?: '')
                    }
                    if (metadata.name && (attrs.image.title || creator)) {
                        mkp.yield(' - ')
                    }

                    def text = "${attrs.image.title? attrs.image.title: ''} ${creator}"
                    mb.span {
                        mkp.yieldUnescaped(sanitiserService.truncateAndSanitise(text, attrs.image.imageIdentifier, 'title+creator', 100))
                    }
                }
            } else {
                if (attrs.image.dataResourceUid || attrs.image.title || creator) {
                    mb.div(class: ['thumb-caption', 'caption-detail', attrs.css ?: ''].findAll().join(' ')) {
                        def output = "${attrs.image.dataResourceUid ? attrs.image.dataResourceUid: ''} ${attrs.image.title ? attrs.image.title :''} ${creator}"
                        mkp.yieldUnescaped(sanitiserService.truncateAndSanitise(output, attrs.image.imageIdentifier, 'drUid+title+creator', 100))
                    }
                }
            }
        }
    }

    def facetDataResourceResult = { attrs, body ->
        def metadata = collectoryService.getResourceLevelMetadata(attrs.dataResourceUid)
        def valueToRender = ""
        if (metadata &&  metadata.name){
            valueToRender = metadata.name
        } else {
            valueToRender = message(code: attrs.dataResourceUid, default: attrs.dataResourceUid)
        }
        def mb = new MarkupBuilder(out)
        mb.span(class: 'resource-name') {
            mkp.yield(valueToRender)
        }
    }

    def sizeInBytes = { attrs, body ->
        if (attrs.size) {
            out << ImageUtils.formatFileSize(attrs.size as Double)
        }
    }

    def formatDate = { attrs, body ->
        def date = attrs.date as Date
        if (date) {
            out << g.formatDate(date: date, format: "dd MMM, yyyy")
        }
    }

    def formatDateTime = { attrs, body ->
        def date = attrs.date as Date
        if (date) {
            out << g.formatDate(date: date, format: "dd MMM, yyyy HH:mm:ss")
        }
    }

    /**
     * @attr active
     * @attr title
     * @attr href
     */
    def menuNavItem = { attrs, body ->
        def active = attrs.active
        if (!active) {
            active = attrs.title
        }
        def current = pageProperty(name:'page.pageTitle')?.toString()

        def mb = new MarkupBuilder(out)
        mb.li(class: active == current ? 'active' : '') {
            a(href:attrs.href) {
//                i(class:'icon-chevron-right') { mkp.yieldUnescaped('&nbsp;')}
                mkp.yield(attrs.title)
            }
        }
    }

    def navSeparator = { attrs, body ->
        out << "&nbsp;&#187;&nbsp;"
    }

    /**
     * @attr criteriaDefinition
     * @attr units
     */
    def criteriaValueControl = { attrs, body ->
        def allDefinitions = searchCriteriaService.criteriaDefinitionList

        def criteriaDefinition = attrs.criteriaDefinition as SearchCriteriaDefinition
        if (criteriaDefinition) {

            // Remove incompatible definitions for those criteria types that can compare against other fields
            allDefinitions.removeAll {
                it.valueType != criteriaDefinition.valueType
            }

            def templateName = criteriaDefinition.valueType.toString()
            if (criteriaDefinition.valueType == CriteriaValueType.NumberRangeLong) {
                templateName = CriteriaValueType.NumberRangeInteger.toString()
            }

            def templatePath = '/criteriaControls/' + templateName[0].toLowerCase() + templateName.substring(1)
            if (groovyPageLocator) {
                if (!groovyPageLocator.findTemplateByPath(templatePath)) {
                    throw new Exception("Could not locate template for criteria value type: " + criteriaDefinition.valueType.toString())
                }
            }

            def allowedValues = []
            if (criteriaDefinition.valueType == CriteriaValueType.StringMultiSelect) {
            }

            out << render(template: templatePath, model: [criteriaDefinition: criteriaDefinition, units: criteriaDefinition.units, value: attrs.value, allowedValues: allowedValues, criteriaDefinitions: allDefinitions])
        }
    }

    def searchCriteriaDescription = { attrs, body ->
        def criteria = attrs.criteria as SearchCriteria
        if (criteria) {
            def mb = new MarkupBuilder(out)
            if (criteria.criteriaDefinition.valueType == CriteriaValueType.Boolean) {
                mb.strong(criteria.criteriaDefinition.description)
            } else {
                mb.strong(criteria.criteriaDefinition.name)
            }
            mb.mkp.yieldUnescaped("&nbsp;")
            mb.mkp.yieldUnescaped(ESSearchCriteriaUtils.format(criteria, { str -> "<strong>${str}</strong>" as String} ))
        }
    }

    /**
     * @attr userId User id
     */
    def userDisplayName = { attrs, body ->
        String userId = attrs.userId as String
        def displayName = ""
        if (userId) {
            displayName = authService.getUserForUserId(userId)?.displayName
        }

        def currentUserId = authService.getUserId()
        out << (sanitiserService.sanitise(displayName ?: userId ?: '&lt;Unknown&gt;'))

        if(currentUserId && currentUserId == userId){
            out << " (thats you!)"
        }
    }

    def userIsUploader = { attrs, body ->
        def currentUserId = authService.getUserId()
        if(attrs.image && attrs.image.uploader && attrs.image.uploader == currentUserId){
            out << body()
        }
    }

    def sanitiseString = { attrs, body ->
        return sanitiseInternal(attrs)
    }

    def sanitise = { attrs, body ->
        out << sanitiseInternal(attrs)
    }

    private String sanitiseInternal(attrs) {
        def value = attrs.value
        def image = attrs.image
        def key = attrs.key
        def length = attrs.length
        def result
        if (image && key) {
            if (length) {
                result = sanitiserService.truncateAndSanitise(value, image, key, length)
            } else {
                result = sanitiserService.sanitise(value, image, key)
            }
        } else {
            if (length) {
                result = sanitiserService.truncateAndSanitise(value, length)
            } else {
                result = sanitiserService.sanitise(value)
            }
        }
        return result
    }

    def imageMetadata = { attrs, body ->
        if (attrs.image[attrs.field]) {
            out << sanitiserService.sanitise(attrs.image[attrs.field])
        } else if (attrs.resource && attrs.resource.imageMetadata && attrs.resource.imageMetadata[attrs.field]) {
            out << sanitiserService.sanitise(attrs.resource.imageMetadata[attrs.field]) + "<small> (resource level metadata) </small>"
        }
    }

    /**
     * @attr markdown defaults to true, will invoke the markdown service
     * @attr tooltipPosition (one of 'topLeft, 'topMiddle', 'topRight', 'bottomLeft', 'bottomMiddle', 'bottomRight')
     * @attr tipPosition (one of 'topLeft, 'topMiddle', 'topRight', 'bottomLeft', 'bottomMiddle', 'bottomRight')
     * @attr targetPosition (one of 'topLeft, 'topMiddle', 'topRight', 'bottomLeft', 'bottomMiddle', 'bottomRight')
     */
    def helpText = { attrs, body ->
        def mb = new MarkupBuilder(out)
        def helpText = (body() as String)?.trim()?.replaceAll("[\r\n]", "");
        if (helpText) {
            // helpText = markdownService.markdown(helpText)
            def attributes = [href:'#', class:'fieldHelp', title:helpText, tabindex: "-1"]
            if (attrs.tooltipPosition) {
                attributes.tooltipPosition = attrs.tooltipPosition
            }
            if (attrs.tipPosition) {
                attributes.tipPosition = attrs.tipPosition
            }
            if (attrs.targetPosition) {
                attributes.targetPosition = attrs.targetPosition
            }

            if (attrs.width) {
                attributes.width = attrs.width
            }

            mb.a(attributes) {
                span(class:'help-container') {
                    mkp.yieldUnescaped('&nbsp;')
                }
            }
        } else {
            mb.mkp.yieldUnescaped("&nbsp;")
        }
    }

    /**
     * @attr metaDataItem The metadata item whose value is to be rendered
     */
    def renderMetaDataValue = { attrs, body ->
        ImageMetaDataItem md = attrs.metaDataItem as ImageMetaDataItem
        if (md) {
            out << sanitiserService.sanitise(new MetaDataValueFormatRules(grailsApplication).formatValue(md))
        }
    }
}
