"""Initialize the sphinx application."""
from sphinx.application import Sphinx


def setup(app: Sphinx) -> None:
    """Setup the sphinx app for these directives.
    
    :param Sphinx app: The sphinx app to operate on.
    """
    from .markdown_domain import MarkdownDomain
    from .markdown_file_input import MarkdownFileInput
    from .markdown_parser import MarkdownParser
    from .transforms import RewriteReferenceHashesTransform
    from .transforms import RewriteTargetRefIdsTransform
    from .transforms import RewriteSectionsIdsTransform
    from .transforms import ImageExternalTargets
    from .transforms import ImageTargetsCaptions
    from .transforms import DocinfoTransform

    app.registry.add_source_input(MarkdownFileInput)
    app.add_source_parser(MarkdownParser)
    app.add_source_suffix('.md', 'markdown')
    app.add_domain(MarkdownDomain)
    app.add_transform(RewriteReferenceHashesTransform)
    app.add_transform(RewriteTargetRefIdsTransform)
    app.add_transform(RewriteSectionsIdsTransform)
    app.add_transform(ImageExternalTargets)
    app.add_transform(ImageTargetsCaptions)
    app.add_transform(DocinfoTransform)
