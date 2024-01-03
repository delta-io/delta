"""Initializes the metadata module."""
from .meta import Meta
from .meta import process_meta_nodes
from .meta_transform import MetaTransform


def setup(app) -> None:
    """
    Register the extension and directive.

    :param app: sphinx.application
    :return dict: Dictionary of additional config values.
    """
    app.add_config_value('metadata_ignore', [], 'env', [list])
    app.add_directive('meta', Meta, override=True)
    app.connect('doctree-resolved', process_meta_nodes)
    app.add_post_transform(MetaTransform)
