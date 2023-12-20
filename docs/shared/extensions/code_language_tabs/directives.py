"""Adds directives for adding language tabs in the docs."""
from docutils import nodes
from sphinx.util.docutils import SphinxDirective
from sphinx.util.nodes import set_source_info


class CodeLanguageTabsError(Exception):
    """Exception for handling code language tab errors."""

    pass


#: Name of the html extension.
extHtml = 'html'
#: Name of markdown extension
extMarkdown = 'markdown'
#: Toggle extension to off.
extOff = 'off'
#: Setting name.
configSettingName = 'code-language-tabs-mode'
#: Literal mode.
modeLiteral = 'literal'
#: Mixed mode.
modeMixed = 'mixed'

#: List of language names available.
_lang_names = {
    # ****************************************************************
    # HTML has its own implementation of this due to technicalion reasons
    # ****************************************************************
    'sql': 'SQL',
    'dotnet': '.NET',
    'xml': 'XML',
    'json': 'JSON',
    'http': 'HTTP',
    'md': 'Markdown',
    'yaml': 'YAML',
    'ini': 'ini',
    'spacy': 'spaCy'
}


def _format_language_name(lang: str) -> str:
    """Format a language name.

    :param lang: Language name to format
    :return: List of language names
    """
    return _lang_names[lang] if lang in _lang_names else lang.capitalize()


def _get_page_language(node: nodes.Element, mode: str) -> str:
    """Get current page language.

    :param node: Current node
    :param mode: Mode for string
    :return: language item of node list
    """
    return node['language' if mode == modeLiteral else 'tabs-lang']


def _format_markdown_node(node: nodes.Element, mode: str) -> nodes.Element:
    """Format a markdown node.

    :param node: Current node
    :param mode: Current mode
    :return: Resulting section of the document
    """
    lang = _get_page_language(node, mode)
    name = _format_language_name(lang)
    title = nodes.title(name, name)
    section = nodes.section('', title, node)
    section['ids'] = ['clt-section-id-' + lang]
    return section


class CodeLanguageTabs(SphinxDirective):
    """Add code language tabs directive to Sphinx."""

    #: (dict) contains the available options for this directive.
    option_spec = {}
    #: (bool) defines the existence of content for this instance.
    has_content = True

    def run(self):
        """Apply directive changes when directive is found.

        .. todo:: This is a little complex for my taste.
        """
        node = nodes.compound()
        node.document = self.state.document
        set_source_info(self, node)

        self.state.nested_parse(self.content, self.content_offset,
                                node)

        if self.config[configSettingName] == extOff:
            return node.children
        else:
            if not node.children:
                raise CodeLanguageTabsError(
                    'code-language-tabs directive may not be empty')

            mode = self._get_directive_mode(node[0])
            if len(node) == 1:
                return (node.children
                        if mode == modeLiteral else node[0].children)

            for i in range(1, len(node)):
                self._validate_against_mode(node[i], mode)

            if self.config[configSettingName] == extMarkdown:
                for child in node:
                    new = _format_markdown_node(child, mode)
                    node.replace(child, new)
            else:
                node['classes'] = ['js-code-language-tabs']
                if mode == modeLiteral:
                    node['classes'].append('js-code-language-tabs--literal')

            return [node]

    def _get_directive_mode(self, node: nodes.Element) -> str:
        if self._is_literal_block(node):
            return modeLiteral
        elif self._is_mixed_content(node):
            return modeMixed
        else:
            raise

    def _validate_against_mode(self, node: nodes.Element, mode: str) -> None:
        if mode == modeLiteral:
            if not self._is_literal_block(node):
                raise CodeLanguageTabsError(
                    ('The code-language-tabs directive must'
                     ' contain only literal_block children'))
        else:
            if not self._is_mixed_content(node):
                raise CodeLanguageTabsError(
                    ('The code-language-tabs directive must'
                     ' contain only ".. lang:: <language>" children'))

    def _is_literal_block(self, node: nodes.Element) -> bool:
        return isinstance(node, nodes.literal_block)

    def _is_mixed_content(self, node: nodes.Element) -> bool:
        return isinstance(node, nodes.compound) and 'tabs-lang' in node


class Lang(SphinxDirective):
    """Add lang directive to Sphinx."""

    #: Current option spec
    option_spec = {}
    #: Does the directive have content
    has_content = True
    #: Number of required arguments
    required_arguments = 1

    def run(self):
        """Run build code when lang directive is found."""
        lang = self.arguments[0]
        node = nodes.compound()

        node.document = self.state.document
        set_source_info(self, node)
        self.state.nested_parse(self.content, self.content_offset,
                                node)

        if self.config[configSettingName] == extOff:
            return node.children
        else:
            if not node.children:
                raise CodeLanguageTabsError('lang directive may not be empty')

            node['classes'] = ['language-' + lang]
            node['tabs-lang'] = lang

            return [node]
