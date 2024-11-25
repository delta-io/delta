"""Transforms the metadata."""
from sphinx.transforms import SphinxTransform
from sphinx import addnodes


class MetaTransform(SphinxTransform):
    """Instantiates the metadata transformations."""

    #: Set the default priority
    default_priority = 10

    def apply(self, **kwargs):
        """Apply the required transformations.

        :params self: Current instance
        :params **kwargs: Keyword arguments'
        """
        for meta in self.document.traverse(addnodes.meta):
            if meta.children:
                meta['content'] = meta.astext()
                meta.children.clear()
