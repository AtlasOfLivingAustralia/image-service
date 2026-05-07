<div class="form-horizontal">

    <div class="control-group">
        <label class="control-label" for="search">Apply an existing tag</label>
        <div class="controls">
            <input type="text" id="search" placeholder="Find tags">
            <button id="btnSearchTags" class="btn"><i class="icon-search"></i>&nbsp;Search</button>&nbsp;OR&nbsp;<button id="btnAddAndSelectTag" class="btn"><i class="icon-plus"></i>&nbsp;Create a new tag</button>
        </div>
    </div>

    <div class="well well-small">
        <div id="tagContainer"></div>
    </div>


    <div class="control-group">
        <div class="controls">
            <button class="btn btn-primary" id="btnSelectTag">Select tag</button>
            <button class="btn" id="btnCancelAttachTag">Cancel</button>
        </div>
    </div>
</div>

<style>
#tagTree {
    height: 300px;
}
</style>

<script>

    $("#search").keydown(function(e) {
        if (e.which == 13) {
            e.preventDefault();
            loadTagTree();
        }
    }).focus();

    $("#btnSearchTags").on('click', function(e) {
        e.preventDefault();
        loadTagTree();
    });

    $("#btnAddAndSelectTag").on('click', function(e) {
        e.preventDefault();
        var parentTagId = getSelectedTagId();
        imgvwr.hideModal();
        imgvwr.createNewTag(parentTagId, function(tagId) {
            if (tagId && imgvwr.onTagSelected) {
                imgvwr.onTagSelected(tagId);
            }
        });
    });

    $("#btnCancelAttachTag").on('click', function(e) {
        e.preventDefault();
        imgvwr.hideModal();
    });

    function selectCurrentTag() {
        var tagId = getSelectedTagId();
        if (tagId && imgvwr.onTagSelected) {
            imgvwr.onTagSelected(tagId);
        }
    }

    $("#btnSelectTag").on('click', function(e) {
        e.preventDefault();
        selectCurrentTag();
    });

    function getSelectedTagId() {
        var tree = $("#tagTree");
        var selected = tree.jstree("get_selected");
        if (selected && selected.length > 0) {
            var selectedNode = tree.jstree("get_node", selected[0]);
            return selectedNode.original.tagId;
        }
        return null
    }

    function loadTagTree() {

        $("#tagContainer").html('<div id="tagTree"></div>');

        var q = $("#search").val();

        $.ajax("${createLink(controller:'webService', action:'getTagModel')}?q=" + q).done(function(rootNodes) {

            if (!rootNodes || rootNodes.length == 0) {
                $("#tagContainer").html("<div style='opacity: 0.5'>(No tags found)</div>");
            }

            var tree = $("#tagTree");

            tree.on("ready.jstree", function (event, data) {
                tree.jstree("open_all");
            }).on("dblclick.jstree", function() {
                selectCurrentTag();
            }).jstree({
                "plugins" : [],
                "core" : {
                    "animation" : 100,
                    data: rootNodes,
                    "themes": {
                        dots: false
                    },
                    multiple: false
                }
            });

        });
    }

    loadTagTree();

</script>