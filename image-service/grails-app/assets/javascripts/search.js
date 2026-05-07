
function selectAllOnPage() {

    imgvwr.showSpinner("Selecting images on page...");
    var imageList = $(".thumbnail[imageId]").map(function() {
        return $(this).attr("imageId");
    }).get();

    $.post("${createLink(controller:'selection', action:'ajaxSelectImages')}", { imageList: imageList } ).done(function() {
        updateSelectionContext();
        $(".thumbnail[imageId]").each(function(e) {
            $(this).find(".chkSelectImage").prop('checked', true);
        });
        imgvwr.hideSpinner();
    });
}

function deselectAllOnPage() {

    imgvwr.showSpinner("Deselecting images...");

    var imageList = $(".thumbnail[imageId]").map(function() {
        return $(this).attr("imageId");
    }).get();

    $.post("${createLink(controller:'selection', action:'ajaxDeselectImages')}", { imageList: imageList } ).done(function() {
        updateSelectionContext();
        $(".thumbnail[imageId]").each(function(e) {
            $(this).find(".chkSelectImage").prop('checked', false);
        });
        imgvwr.hideSpinner();
    });
}

function getheight (images, width) {
    width -= images.length * 5;
    var h = 0;
    for (var i = 0; i < images.length; ++i) {
        if ($(images[i]).data('width') === undefined) {
            $(images[i]).data('width', $(images[i]).width());
        }
        if ($(images[i]).data('height') === undefined) {
            $(images[i]).data('height', $(images[i]).height());
        }
        //console.log("original = " + $(images[i]).data('width') + '/' + $(images[i]).data('height'));
        h += $(images[i]).data('width') / $(images[i]).data('height');
    }
    //console.log("row count = " + images.length + " row height = " + width / h);
    return width / h;
};

function setheight (images, height) {
    for (var i = 0; i < images.length; ++i) {
        //console.log("setting width to " + height * $(images[i]).data('width') / $(images[i]).data('height'));
        $(images[i]).css({
            width: height * $(images[i]).data('width') / $(images[i]).data('height'),
            height: height
        });
    }
};

function layoutImages (maxHeight) {

    console.log("Layout called");

    var size = $imageContainer.innerWidth(),
        n = 0,
        images = $imageContainer.find('img');
    if (maxHeight === undefined) {
        maxHeight = MAX_HEIGHT;
    }

    w: while (images.length > 0) {
        for (var i = 1; i < images.length + 1; ++i) {
            var slice = images.slice(0, i);
            var h = self.getheight(slice, size);
            if (h < maxHeight) {
                self.setheight(slice, h);
                n++;
                images = images.slice(i);
                continue w;
            }
        }

        self.setheight(slice, Math.min(maxHeight, h));
        n++;
        break;
    }
}