<!doctype html>
<html>
    <head>
        <meta name="layout" content="adminLayout"/>
        <meta name="section" content="home"/>
        <title>ALA Images - Admin - Tags</title>
    </head>
    <body>

        <style>
            #searchTags {
                margin-bottom: 0 !important;
            }
        </style>
        <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jstree/3.2.1/themes/default/style.min.css" />
        <script src="https://cdnjs.cloudflare.com/ajax/libs/jstree/3.2.1/jstree.min.js"></script>

        <content tag="pageTitle">Tags</content>
        <content tag="adminButtonBar" />

        <div class="row">
            <div class="col-md-12">
                <form class="form-inline">
                    <button class="btn btn-success" id="btnCreateNewTag"><i class="glyphicon glyphicon-plus "> </i>&nbsp;Add</button>
                    <button class="btn btn-default" id="btnRenameSelectedTag">Rename</button>
                    <button class="btn btn-danger" id="btnDeleteSelectedTag">
                        <i class="glyphicon glyphicon-remove glyphicon-white"></i>&nbsp;Delete
                    </button>
                    <button class="btn btn-default pull-right" id="btnUploadTags"><i class="glyphicon glyphicon-upload"> </i>&nbsp;Upload tags from CSV file</button>
                    <input type="text" id="searchTags" class="form-control input" placeholder="Find tags">
                    <button id="btnSearchTags" class="btn btn-default"><i class="glyphicon glyphicon-search"> </i>&nbsp;Search</button>
                </form>
            </div>
        </div>

        <div class="row" style="margin-top:10px;">
            <div class="col-md-12">
                <div id="tagContainer" class="well well-small">
                    <img:spinner />
                </div>
            </div>
        </div>

        <div id="tagModal" class="modal fade" role="dialog">
            <div class="modal-dialog">
                <div class="modal-content">
                    <div class="modal-header">
                        <button type="button" class="close" data-dismiss="modal">&times;</button>
                        <h4 class="modal-title">Tags</h4>
                    </div>
                    <div class="modal-body">

                    </div>
                    <div class="modal-footer">
                    </div>
                </div>
            </div>
        </div>

    <script>

        $(document).ready(function() {

            $("#btnCreateNewTag").on('click', function(e) {
                e.preventDefault();
                createTag();
            });

            $("#btnRenameSelectedTag").on('click', function(e) {
                e.preventDefault();
                renameSelectedTag();
            });

            $("#btnDeleteSelectedTag").on('click', function(e) {
                e.preventDefault();
                deleteSelectedTag();
            });

            $("#btnUploadTags").on('click', function(e) {
                e.preventDefault();
                $.ajax("${createLink(action:'uploadTagsFragment')}").done(function(content) {
                    $("#tagModal .modal-title").html("Upload tags - select a file to upload");
                    $("#tagModal .modal-body").html(content);
                });
                $('#tagModal').modal('show');
            });

            $("#searchTags").keydown(function(e) {
                if (e.which == 13) {
                    e.preventDefault();
                    loadTagTree();
                }
            }).focus();

            $("#btnSearchTags").on('click', function(e) {
                e.preventDefault();
                loadTagTree();
            });

            $('#tagModal').on('hidden.bs.modal', function () {
                loadTagTree();
            })

            loadTagTree();
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

        function deleteSelectedTag() {
            var tagId = getSelectedTagId();
            if (tagId) {
                $.ajax("${createLink(controller:'tag', action:'deleteTagFragment')}?tagID=" +tagId).done(function(content) {
                    $("#tagModal .modal-title").html("Delete tag");
                    $("#tagModal .modal-body").html(content);
                });
                $('#tagModal').modal('show');
            }
        }

        function renameSelectedTag() {
            var tagId = getSelectedTagId();
            if (tagId) {
                $.ajax("${createLink(controller:'tag', action:'renameTagFragment')}?tagID=" +tagId).done(function(content) {
                    $("#tagModal .modal-title").html("Rename tag");
                    $("#tagModal .modal-body").html(content);
                });
                $('#tagModal').modal('show');
            }
        }

        function createTag() {
            var parentTagId = getSelectedTagId();
            $.ajax("${createLink(controller:'tag', action:'createTagFragment')}?parentTagID=" + parentTagId).done(function(content) {
                $("#tagModal .modal-body").html(content);
            });

            $('#tagModal').modal('show');
        }

        function loadTagTree() {

            $("#tagContainer").html('<div id="tagTree"></div>');

            var q = $("#searchTags").val();

            $.ajax("${createLink(controller:'webService', action:'getTagModel')}?q=" + q).done(function(rootNodes) {

                var tree = $("#tagTree");

                tree.on("ready.jstree", function (event, data) {
                    tree.jstree("open_all");
                }).on("move_node.jstree", function(e, data) {

                    var newParentTagId = -1;
                    if (data.parent) {
                        var parentNode = tree.jstree("get_node", data.parent);
                        if (parentNode && parentNode.original) {
                            newParentTagId = parentNode.original.tagId;
                        }
                    }

                    var targetTagId = data.node.original.tagId;
                    moveTag(targetTagId, newParentTagId);
                }).jstree({
                    "plugins" : ["dnd", "checkbox"],
                    "core" : {
                        "animation" : 100,
                        data: rootNodes,
                        "themes": {
                            dots: false
                        },
                        "check_callback" : true,
                        multiple: false
                    },
                    "checkbox" : {
                        three_state: false,
                        whole_node: false
                    },
                    "dnd" : {
                        check_while_dragging: false
                    }
                });

            });
        }

        function moveTag(targetTagId, newParentTagId) {
            var url = "${createLink(controller:'webService', action:'moveTag')}?targetTagID=" + targetTagId + "&newParentTagID=" + newParentTagId;
            $.ajax(url).done(function() {
                loadTagTree();
            });
        }
    </script>
    </body>
</html>



