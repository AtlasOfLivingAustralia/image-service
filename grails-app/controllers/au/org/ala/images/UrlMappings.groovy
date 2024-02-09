package au.org.ala.images

class UrlMappings {

	static mappings = {
        "/$controller/$action?/$id?(.$format)?"{
            constraints {
                // apply constraints here
            }
        }

        "/ws/image/$imageID?(.$format)?"(controller: "webService") {
                action = [GET: 'getImageInfo', DELETE: 'deleteImageService', HEAD: 'getImageInfo']
        }
        "/ws/updateMetadata/$imageIdentifier"(controller: "webService", action: "updateMetadata")
        "/ws/getImageInfo/$imageID"(controller: "webService", action:'getImageInfo')
        "/ws/repositoryStatistics"(controller: "webService", action:'getRepositoryStatistics')
        "/ws/repositorySizeOnDisk"(controller: "webService", action:'getRepositorySizeOnDisk')
        "/ws/backgroundQueueStats"(controller: "webService", action:'getBackgroundQueueStats')
        "/ws/metadatakeys"(controller: "webService", action:'getMetadataKeys')
        "/ws/batchstatus"(controller: "webService", action:'getBatchStatus')
        "/ws/imageInfoForList"(controller: "webService", action: "getImageInfoForIdList")

        "/ws/batch/$id"(controller: "batch", action: "status")
        "/ws/batch/status/$id"(controller: "batch", action: "status")
        "/ws/batch/dataset/$dataResourceUid"(controller: "batch", action: "statusForDataResource")
        "/ws/batch/dataresource/$dataResourceUid"(controller: "batch", action: "statusForDataResource")
        "/ws/batch/upload"(controller: "batch", action: "upload")

        "/ws/$action?/$id?(.$format)?" {
            controller = "webService"
        }

        "/ws/$action?" {
            controller = "webService"
        }

        "/ws/api"(controller: 'openApi', action: 'openapi')
        name api_doc: "/ws/"(controller: 'openApi', action: 'index')
        "/ws"(controller: 'openApi', action: 'index')

        // legacy URLS
        "/image/imageTooltipFragment"(controller: "image", action: "imageTooltipFragment")
        "/image/proxyImageThumbnail"(controller: "image", action: "proxyImageThumbnail")
        "/image/proxyImageThumbnailLarge"(controller: "image", action: "proxyImageThumbnailType")
        "/image/proxyImageTile"(controller: "image", action: "proxyImageTile")
        "/image/proxyImage"(controller: "image", action: "proxyImage")
        "/proxyImageThumbnail"(controller: "image", action: "proxyImageThumbnail")
        "/proxyImageThumbnailLarge"(controller: "image", action: "proxyImageThumbnailType")
        "/proxyImageTile"(controller: "image", action: "proxyImageTile")
        "/proxyImage"(controller: "image", action: "proxyImage")

        "/image/viewer"(controller:"image", action: "viewer")
        "/image/view/$id"(controller:"image", action: "viewer")
        "/image/viewer/$id"(controller:"image", action: "viewer")

        // homogeneous URLs
        "/image/$id/thumbnail"(controller: "image", action: "proxyImageThumbnail")
        "/image/$id/large"(controller: "image", action: "proxyImageThumbnailType", params: ['thumbnailType': 'large'])
        "/image/$id/tms/$z/$x/${y}.png"(controller: "image", action: "proxyImageTile")
        "/image/$id/original"(controller: "image", action: "getOriginalFile")

        // take over old apache paths
        "/store/$a/$b/$c/$d/$id/thumbnail"(controller: "image", action: "proxyImageThumbnail")
        "/store/$a/$b/$c/$d/$id/thumbnail_$thumbnailType"(controller: "image", action: "proxyImageThumbnailType")
        "/store/$a/$b/$c/$d/$id/tms/$z/$x/${y}.png"(controller: "image", action: "proxyImageTile")
        "/store/$a/$b/$c/$d/$id/original"(controller: "image", action: "getOriginalFile")

        "/admin/image/$imageId"(controller: "admin", action: "image")
        "/image/details"(controller: "image", action: "details")

        //analytics
        "/ws/analytics"(controller: "analytics", action: "byAll")
        "/ws/analytics/$dataResourceUID"(controller: "analytics", action: "byDataResource")
        "/ws/analytics/dataResource/$dataResourceUID"(controller: "analytics", action: "byDataResource")

        name image_url: "/image/$imageId(.$format)?"(controller: "image", action: "details")
        name image_ws_url: "/ws/image/$imageId"(controller: "webService", action: "getImageInfo")

        //tags
        "/ws/tags"(controller: "webService", action: "getTagModel")
            "/ws/tagsWS"(controller: "webService", action: [GET: "getTagModelWS"])
        "/ws/tag"(controller: "webService", action: "createTagByPath")
            "/ws/tagWS"(controller: "webService", action: [PUT: "createTagByPathWS"])

        "/ws/tag/$tagID/rename"(controller: "webService", action: "renameTag")
            "/ws/tag/$tagID/renameWS"(controller: "webService", action: "renameTagWS")
        "/ws/tag/$tagId/move"(controller: "webService", action: "moveTag")
            "/ws/tag/move"(controller: "webService", action: "moveTag")
            "/ws/tag/moveWS"(controller: "webService", action: "moveTagWS")
        "/ws/tag/$tagID/images"(controller: "webService", action: "getImagesForTag")
            "/ws/tag/$tagId"(controller: "webService", action: "deleteTag")
            "/ws/tag/delete/$tagId"(controller: "webService", action: [DELETE: "deleteTagWS"])

        "/ws/images/keyword/$keyword"(controller: "webService", action: "getImagesForKeyword")
        "/ws/images/tag/$tagID"(controller: "webService", action: "getImagesForTag")

        "/ws/tag/$tagId/image/$imageId"(controller: "webService"){
           action = [GET: 'attachTagToImage', PUT: 'attachTagToImage', DELETE: 'detachTagFromImage']
        }
            "/ws/tag/$tagId/imageWS/$imageId"(controller: "webService"){
                    action = [GET: 'attachTagToImageWS', PUT: 'attachTagToImageWS', DELETE: 'detachTagFromImageWS']
            }
        "/ws/tag/$tagId/images"(controller: "webService", action:"getImagesForTag")

        "/"(controller:'search', action:'list')
        "500"(view:'/error')
	}
}
