package au.org.ala.images

class TagUtils {

    public static List<Tag> getLeafTags(List<Tag> tags) {
        def tree = buildTagTree(tags)
        def leaves = []

        def v = { node ->
            if (!node.children && node.tag) {
                leaves << node.tag
            }
        } as ITagTreeNodeVisitor

        tree.traverseDepthFirst(v)
        return leaves
    }

    private static TagTreeNode buildTagTree(List<Tag> tags) {
        def root = new TagTreeNode(null, null, '')
        tags?.each { tag ->
            def bits = tag.path.split(TagConstants.TAG_PATH_SEPARATOR)
            TagTreeNode p = root
            bits.each { pathElement ->
                if (pathElement) {
                    def child = p.findChild(pathElement)
                    if (!child) {
                        child = new TagTreeNode(tag, p, pathElement)
                        p.children.add(child)
                    }
                    p = child
                }
            }
        }

        return root
    }

    private static class TagTreeNode {
        TagTreeNode parent
        List<TagTreeNode> children
        Tag tag
        String label

        public TagTreeNode(Tag tag, TagTreeNode parent, String label) {
            this.tag = tag
            this.label = label
            this.parent = parent
            this.children = new ArrayList<TagTreeNode>()
        }

        public TagTreeNode findChild(String pathSegment) {
            return children.find { it.label.equals(pathSegment) }
        }

        public void dump(Closure emitNode) {
            dumpNode(0, this, emitNode)
        }

        public void traverseDepthFirst(ITagTreeNodeVisitor visitor) {
            traverseNode(this, visitor)
        }

        private void traverseNode(TagTreeNode node, ITagTreeNodeVisitor visitor) {
            visitor.visit(node);
            node.children?.each {
                traverseNode(it, visitor)
            }
        }

        private static void dumpNode(int level, TagTreeNode node, Closure emit) {
            emit(level, node)
            node.children?.each { child ->
                dumpNode(level+1, child, emit)
            }
        }

        public String toString() {
            return "${label ?: 'ROOT'} (${children.join(",")})"
        }
    }

    public static interface ITagTreeNodeVisitor {
        void visit(TagTreeNode node);
    }

}
