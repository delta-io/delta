"""Contains the MarkdownStateMachine class."""
from docutils.statemachine import StateMachineWS
from docutils.parsers.rst.states import Struct
from docutils.parsers.rst import languages
from .states import MarkdownInliner


class MarkdownStateMachine(StateMachineWS):
    """Instantiates the MarkdownStateMachine class."""

    def run(self, input_lines, document, input_offset=0, match_titles=True):
        """Parse `input_lines` and modify the `document` node in place.

        Extend `StateMachineWS.run()`: set up parse-global data and
        run the StateMachine.

        :param self: Current instance.
        :param input_lines: Lines for input.
        :param document: Current document.
        :param input_offset: Offset the input by this much.
        :param match_titles: Do we match titles or not.
        """
        self.language = languages.get_language(
            document.settings.language_code)
        self.match_titles = match_titles

        inliner = MarkdownInliner()
        inliner.init_customizations(document.settings)

        self.memo = Struct(document=document,
                           reporter=document.reporter,
                           language=self.language,
                           title_styles=[],
                           section_level=0,
                           section_bubble_up_kludge=False,
                           inliner=inliner)
        self.document = document
        self.attach_observer(document.note_source)
        # self.reporter = self.memo.reporter
        self.node = document
        results = StateMachineWS.run(self, input_lines, input_offset,
                                     input_source=document['source'])
        assert results == [], ('MarkdownStateMachine.run() '
                               'results should be empty!')
        self.node = self.memo = None  # remove unneeded references
