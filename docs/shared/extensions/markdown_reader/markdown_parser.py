"""Contains the MardownParser class."""
from docutils import frontend, nodes
from sphinx.parsers import Parser

from .markdown_state_machine import MarkdownStateMachine
from .states import state_classes


class MarkdownParser(Parser):
    """Instantiate the MarkdownParser class."""

    #: List of supported languages.
    supported = ['markdown']
    #: Define the settings spec.
    settings_spec = (
        'reStructuredText Parser Options',
        None,
        [('Set number of spaces for tab expansion (default 8).',
          ['--tab-width'],
          {'metavar': '<width>', 'type': 'int', 'default': 8,
           'validator': frontend.validate_nonnegative_int})]
    )
    #: Add a config section for this parser.
    config_section = 'markdown parser'
    #: Add dependencies to this parser.
    config_section_dependencies = ['parsers']

    def parse(self, inputstring, document):
        """Parse the markdown input.

        :param self: Current instance.
        :param inputstring: The string to parse.
        :param document: Current document.
        """
        self.setup_parse(inputstring, document)
        self.statemachine = MarkdownStateMachine(
            initial_state='Body',
            state_classes=state_classes,
            debug=document.reporter.debug_flag, )
        self.statemachine.run(inputstring, document)
        self.finish_parse()

    def finish_parse(self) -> None:
        """Complete the parsing process.

        :param self: Current instance.
        :return None:
        """
        super().finish_parse()
        self._process_footnote_references()
        self._clean_unrecognized_internal_references()
        self._clean_unrecognized_substitutions()

    def _process_footnote_references(self):
        """Process footnot references.

        .. todo: Nested for loops are bad.

        :param self: Current instance.
        """
        footnotes = self._collect_footnotes()
        for ref_name in self.document.footnote_refs:
            if ref_name in footnotes:
                for ref_node in self.document.footnote_refs[ref_name]:
                    ref_node['uri'] = footnotes[ref_name]
            else:
                for ref_node in self.document.footnote_refs[ref_name]:
                    self._replace_with_plain_text(
                        ref_node, ref_node.rawsource[:-1])

    def _collect_footnotes(self):
        """Collect footnotes into a list for parsing.

        .. todo:: Nested for loops are still bad.

        :param self: Current instance.
        :return refs: List of footnot references."""
        refs = {}
        for fn in self.document.footnotes:
            ref = fn.traverse(nodes.reference)
            if ref:
                for id in fn['names']:
                    refs[id] = ref[0]['refuri']
        return refs

    def _clean_unrecognized_internal_references(self):
        """Clean bad internal references.

        :param self: Current instance.
        """
        reflist = self.document.traverse(nodes.reference)
        for ref_node in reflist:
            if ('refname' in ref_node and
                    ref_node['refname'] not in self.document.nameids):
                self._replace_with_plain_text(ref_node, ref_node.rawsource)

    def _clean_unrecognized_substitutions(self):
        """Drop unrecognized substitations.

        .. todo: Nested for loops.

        :param self: Current instance.
        """
        subreflist = self.document.traverse(nodes.substitution_reference)
        for ref in subreflist:
            self._clean_substitution(ref)

        for subdef in self.document.substitution_defs:
            subreflist = self.document.substitution_defs[subdef].traverse(
                nodes.substitution_reference)
            for subref in subreflist:
                self._clean_substitution(subref)

    def _clean_substitution(self, ref):
        """Run the clean substitutions.

        :param self: Current instance.
        :param ref: List of references to clean.
        """
        defs = self.document.substitution_defs
        normed = self.document.substitution_names
        refname = ref['refname']
        if refname not in defs and refname.lower() not in normed:
            self._replace_with_plain_text(ref, ref.rawsource)

    @staticmethod
    def _replace_with_plain_text(node, text):
        """Replace items with plain text.

        :param node: Current node.
        :param text: Text to replace."""
        parent = node.parent
        ind = parent.children.index(node)
        parent.children.remove(node)
        parent.insert(ind, nodes.Text(text, text))
