"""Class for metdatada."""

from docutils import nodes
from docutils.statemachine import StringList
from sphinx import addnodes
from sphinx.directives import patches
from sphinx.util.docutils import SphinxDirective


def process_meta_nodes(app, doctree, fromdocname):
    """Process meta nodes after normal resolution."""
    ignored_names = app.config.metadata_ignore

    for node in doctree.traverse(addnodes.meta):
        if node.attributes.get('name') in ignored_names:
            node.parent.remove(node)


class Meta(patches.Meta, SphinxDirective):
    """Class for Meta directive objects."""

    def run(self):
        """Run required directive code.

        :param self: Current instance.
        """
        result = super().run()

        output = []
        for node in result:
            if (isinstance(node, nodes.pending) and
                    isinstance(node.details['nodes'][0], addnodes.meta)):
                meta = node.details['nodes'][0]
                lines = StringList([meta['content'], ''])
                self.state.nested_parse(lines, self.content_offset, meta)
                output.append(meta)

        return output
