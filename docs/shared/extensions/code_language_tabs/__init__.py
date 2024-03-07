"""Code language tabs initialization."""
from sphinx.application import Sphinx
from .directives import CodeLanguageTabs, Lang


def setup(app: Sphinx):
    """Set up the required directives and configs.

    :param Sphinx app: The current sphinx app.
    """
    app.add_directive('code-language-tabs', CodeLanguageTabs)
    app.add_directive('lang', Lang)
    app.add_config_value('code-language-tabs-mode', 'html', True)
