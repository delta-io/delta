"""Contains classes snad functions related to state machines."""
import re

import docutils.parsers.rst.states as states
from docutils import nodes, statemachine
from docutils.parsers.rst import tableparser
from docutils.parsers.rst.states import MarkupError
from docutils.statemachine import StringList
from docutils.utils import escape2null, unescape
from markdown_utils import handle_explicit_header, make_target_name
from sphinx import addnodes

from .markdown_table_parser import MarkdownTableParser


class MarkdownState(states.RSTState):
    """Instantiates the MarkdownState class."""
    ws_patterns = {'blank': ' *$'}
    ws_initial_transitions = ['blank']

    def __init__(self, state_machine, debug):
        """Initializes the instance.

        :param self: Current instance.
        :param state_machine: The state_machine to work on.
        :param bool debug: Is this a debug operation.
        """
        super().__init__(state_machine, debug)
        self.nested_sm_kwargs = {'state_classes': state_classes,
                                 'initial_state': 'Body'}


class Body(states.Body, MarkdownState):
    """Instantiates the body class."""
    #: List of regular expressions used in the processing of states.
    patterns = {
        'bullet': r'^\s{0,3}([-+*])( +|$)',
        'enumerator': r'^\s{0,3}(%(parens)s|%(rparen)s|%(period)s)( +|$)' % states.Body.pats,
        'substitution_def': r'^\s*!\[Replace\]\s+(?P<name>\<[^>]+\>)',
        'explicit_markup': states.Body.patterns['explicit_markup'],
        'figure': r'^\!\[',
        'field_marker': states.Body.patterns['field_marker'],
        'title': '^(#{1,6}) (.+)$',
        'explicit_target': r'\<a id=\"([\w\d\-_]+)\"\>\s*\<\/a\>\s*$',
        'footnote_or_target': r'^\[([^^\]]+|#|\*)\]:\s*([^\s\"]+)(?:\s\"(.+)\")?$',
        'literal_block': r'^\s{0,3}(```|~~~)\s*([^`\s]+)?\s*$',
        'block_quote': r'^\s{0,3}>\s?(.*)$',
        'meta': '^---$',
        'grid_table_top': states.Body.grid_table_top_pat,
        'simple_table_top': states.Body.simple_table_top_pat,
        'markdown_table_top': r'^\|(?:\s*[^\|]+\s*\|)+$',
        'text': r''
    }
    #: List of beginning transitions to be used in states.
    initial_transitions = (
        'bullet',
        'enumerator',
        'substitution_def',
        'explicit_markup',
        'figure',
        'field_marker',
        'title',
        'explicit_target',
        'footnote_or_target',
        'literal_block',
        'block_quote',
        'meta',
        'grid_table_top',
        'simple_table_top',
        'markdown_table_top',
        'text'
    )
    #: RE for markdown table separators.
    markdown_table_sep_pat = re.compile(r'^\|(?:\s*-+\s*\|)+$')
    #: RE for explicit target separators.
    explicit_target_pat = re.compile(patterns['explicit_target'][:-1])
    #: Footnote digits pattern.
    footnote_digits = re.compile(r'^(?:\d+|#|\*)$')

    def __init__(self, state_machine, debug=False):
        """Initializes the instance.

        :param self: Current instance.
        :param state_machine: State machine.
        :param debug: Is this is a debug session?
        """
        super().__init__(state_machine, debug)
        self.explicit.constructs = [
            (states.Body.substitution_def,
             re.compile(r"""
                              \.\.[ ]+          # explicit markup start
                              \<                # substitution indicator
                              (?![ ]|$)         # first char. not space or EOL
                              """, re.VERBOSE | re.UNICODE)),
            (states.Body.directive,
             re.compile(r"""\.\.[ ]+          # explicit markup start
                        (%s)              # directive name
                        [ ]?              # optional space
                        ::                # directive delimiter
                        ([ ]+|$)          # whitespace or end of line
                        """ % states.Inliner.simplename, re.VERBOSE | re.UNICODE))]

        self.explicit.patterns = states.Struct(
            substitution=re.compile(r"""
                                          (
                                            (?![ ])          # first char. not space
                                            (?P<name>.+?)    # substitution text
                                            %(non_whitespace_escape_before)s
                                            \>               # close delimiter
                                          )
                                          ([ ]+|$)           # followed by whitespace
                                          """ % vars(states.Inliner),
                                    re.VERBOSE | re.UNICODE),
        )

    def _register_explicit_targets(self, title, lineno, include_default=False):
        """Register explicit targets.

        :param self: Current instance.
        :param title: Title of the target.
        :param lineno: Current line number of the document.
        :param include_default: Does this include default settings?
        :return title: Return the adjusted title.
        """
        target_candidates = []
        target_name = self.explicit_target_pat.search(title)
        while target_name:
            target_candidates.append((target_name[0], target_name[1]))
            title = title[target_name.end(0):]
            target_name = self.explicit_target_pat.search(title)

        if target_candidates:
            default_target = nodes.make_id(title)
            for match, name in target_candidates:
                if name != default_target or include_default:
                    target = nodes.target(match, '')
                    name = make_target_name(self.document, name)
                    self.add_target(name, None, target, lineno)
                    self.parent += target

        return title

    def title(self, match, context, next_state):
        """Get a title

        :param self: Current instance.
        :param match: RE match expression.
        :param context: Current context.
        :param next_state: Next state of the system.
        :return: Two empty lists and the string 'Body'
        """
        lineno = self.state_machine.abs_line_number()
        self.section(match[2], match.string, str(len(match[1])), lineno, [])
        return [], 'Body', []

    def explicit_target(self, match, context, next_state):
        """Set an explicit target.

        :param self: Current instance.
        :param match: RE match.
        :param context: Current context.
        :param next_state: Next state of the system.
        :return: Two empty lists and the next state.
        """
        lineno = self.state_machine.abs_line_number()
        target = nodes.target(match[0], '')
        name = make_target_name(self.document, match[1])
        self.add_target(name, None, target, lineno)
        self.parent += target
        return [], next_state, []

    def bullet(self, match, context, next_state):
        """Create a bullet in a list.

        :param self: Current instance.
        :param match: RE match.
        :param context: Current context.
        :param next_state: Next state of the system.
        :return context, next_state, result: Return the context,
            next state, and resulting bullet.
        """
        self.extract_list_item_from_context(context)

        context, next_state, result = super().bullet(match, context, next_state)

        bullet = self.parent.children[-1]
        bullet['bullet'] = match[1]

        return context, next_state, result

    def list_item(self, indent):
        """Create a list item.

        :param self: Current instance.
        :param indent: Level of indent for the item.
        :return listitem, blank_finish: Current list item or a blank.
        """
        indented, indent, line_offset, blank_finish = (
            self.state_machine.get_first_known_indented(indent))
        listitem = nodes.list_item('\n'.join(indented))
        if indented:
            self.nested_parse(indented, input_offset=line_offset,
                              node=listitem)
        return listitem, blank_finish

    def is_enumerated_list_item(self, ordinal, sequence, format):
        """Check validity based on the ordinal value and the second line.

        Return true if the ordinal is valid and the second line is blank,
        indented, or starts with the next enumerator or an auto-enumerator.

        .. todo:: This is a real mess.

        :param self: Current instance.
        :param ordinal: Ordinal position in the list.
        :param sequence: Sequence of the list.
        :param format: Format of the list.
        :return None or 1: Multiple exists from a method are bad.
        """
        if ordinal is None:
            return None
        try:
            next_line = self.state_machine.next_line()
        except EOFError:  # end of input lines
            self.state_machine.previous_line()
            return 1
        else:
            self.state_machine.previous_line()
        if not next_line[:1].strip():  # blank or indented
            return 1
        result = self.make_enumerator(ordinal + 1, sequence, format)
        if result:
            next_enumerator, auto_enumerator = result
            formatinfo = self.enum.formatinfo[format]
            current_enumerator = formatinfo.prefix + \
                str(ordinal) + formatinfo.suffix + ' '
            try:
                if (next_line.startswith(next_enumerator) or
                        next_line.startswith(auto_enumerator) or
                        next_line.startswith(current_enumerator)):
                    return 1
            except TypeError:
                pass
        return None

    def enumerator(self, match, context, next_state):
        """Enumerates lists.

        :param self: Current instance.
        :param match: RE match.
        :param context: Current context.
        :param next_state: Next state of the system.
        :return context, next_state, result: Return the context, next state
             and result.
        """
        self.extract_list_item_from_context(context)
        context, next_state, result = super().enumerator(match, context, next_state)
        return context, next_state, result

    def footnote_or_target(self, match, context, next_state):
        """Decide if we're looking at a footnote or a target.

        :param self: Current instance.
        :param match: RE match.
        :param context: Current context.
        :param next_state: Next state of the system.
        :return: Either a footnote or a target.
        """
        caption = match[1]
        if self.footnote_digits.match(caption):
            return self.footnote(match, context, next_state)
        else:
            return self.target(match, context, next_state)

    def footnote(self, match, context, next_state):
        """Process a footnote.

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system.
        :return: Two blank lists and the next state.
        """
        nodelist, blank_finish = super().footnote(match)
        self.nested_parse(StringList(
            initlist=[match[2]]), input_offset=0, node=nodelist[0])
        self.parent += nodelist
        return [], next_state, []

    def target(self, match, context, next_state):
        """Process a target.

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two blank lists and the next state
        """
        target = nodes.target(match.string, '', refname=match[1])
        self.add_target(match[1], match[2], target,
                        self.state_machine.abs_line_number())
        if (match.lastindex > 2 and match[match.lastindex]):
            # image targets with caption
            target['figure_title'] = match[match.lastindex]
        self.parent += target
        return [], next_state, []

    def figure(self, match, context, next_state):
        """Process a figure

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: The next state of the system
        :return: Two blank lists and the next state of the system
        """
        src, srcline = self.state_machine.get_source_and_line()
        figure_node = nodes.figure()
        figure_node.src = src
        figure_node.srcline = srcline
        textnodes, messages = self.inline_text(
            self.state_machine.line, srcline)
        figure_node += textnodes
        figure_node += messages
        self.parent += figure_node
        return [], next_state, []

    def extract_list_item_from_context(self, context):
        """Extract a list item from the context.

        :param self: Current instance
        :param context: Current context
        """
        if context:
            paragraph, literalnext = self.paragraph(
                context, self.state_machine.abs_line_number() - 1)
            self.parent += paragraph

    def literal_block(self, match, context, next_state):
        """Process a literal block

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two blank lists and the next state.
        :raises EOFError:
        """
        fence = match[1]
        lineno = self.state_machine.abs_line_number()
        end_line = re.compile(r'^\s{{0,3}}{}\s*$'.format(fence))
        contents = []
        try:
            while True:
                self.state_machine.next_line()
                if end_line.match(self.state_machine.line):
                    text = '\n'.join(contents)
                    block = nodes.literal_block(text, text)
                    if match.lastindex > 1:
                        block['language'] = match[2]
                    self.parent += block
                    break
                else:
                    contents.append(self.state_machine.line)
        except EOFError:
            text = '\n'.join(contents)
            msg = self.reporter.error(
                'Non-closed literal block',
                nodes.literal_block(text, text),
                line=lineno)
            self.parent += msg
        return [], next_state, []

    def block_quote(self, match, context, next_state):
        """Process a block quote

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two blank lists and the next state
        """
        def parse_block(lines, sources):
            """Parse a block of the quote

            :param lines: Lines to be processed
            :param sources: Sources to be processed
            """
            list = StringList(lines)
            blockquote = nodes.block_quote('\n'.join(sources))
            self.nested_parse(list, 0, blockquote)
            self.parent += blockquote

        line_start = re.compile(self.patterns['block_quote'])
        lines = [match[1]]
        sources = [match.string]

        try:
            while True:
                self.state_machine.next_line()
                match = line_start.match(self.state_machine.line)
                if match:
                    lines.append(match[1])
                    sources.append(match.string)
                else:
                    parse_block(lines, sources)
                    break
        except EOFError:
            parse_block(lines, sources)

        return [], next_state, []

    def meta(self, match, context, next_state):
        """Return meta field

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two blank lists and the string Meta.
        """
        return [], 'Meta', []

    def isolate_markdown_table(self):
        """Isolate a markdown table

        .. todo:: This is also a mess.

        :param self: Current instance
        :return: None and a list of messages.
        """
        messages = []
        try:
            block = self.state_machine.get_text_block(flush_left=True)
        except statemachine.UnexpectedIndentationError as err:
            block, src, srcline = err.args
            messages.append(self.reporter.error('Unexpected indentation.',
                                                source=src, line=srcline))

        block_is_table = True
        if len(block) < 3 or not self.markdown_table_sep_pat.match(block[1]):
            block_is_table = False
        else:
            reg = re.compile(self.patterns['markdown_table_top'])
            for i in range(2, len(block) - 1):
                if not reg.match(block[i]):
                    block_is_table = False
                    break

        if block_is_table:
            return block, messages
        else:
            self.state_machine.previous_line(len(block))
            return None, messages

    def markdown_table_top(self, match, context, next_state):
        """Handle the top row of a markdown table

        :param self: Current instance
        :param match: RE match
        :param context: Curent context
        :param next_state: Next state of the system
        :return: Two blank lists and the next state or string Text.
        """
        block, messages = self.isolate_markdown_table()
        if block:
            try:
                parser = MarkdownTableParser()
                tabledata = parser.parse(block)
                tableline = (self.state_machine.abs_line_number() - len(block)
                             + 1)
                table = self.build_table(tabledata, tableline)
                nodelist = [table] + messages
            except tableparser.TableMarkupError as err:
                nodelist = self.malformed_table(block, ' '.join(err.args),
                                                offset=err.offset) + messages
            self.parent += nodelist
            return [], next_state, []
        else:
            return [], 'Text', []

    def new_subsection(self, title, lineno, messages):
        """Create a new subsection

        :param self: Current instance
        :param title: Title of the section
        :param lineno: Current line number of the title
        :param messages: List of messages related to the line
        :raises EOFError: 
        """
        is_top_level_section = self.memo.section_level == 0
        try:
            title = self._register_explicit_targets(
                title, lineno, is_top_level_section)
            super().new_subsection(title, lineno, messages)
            if title and not is_top_level_section:
                handle_explicit_header(
                    self.document, self.parent.children[-1], title)
        except EOFError:
            if title and not is_top_level_section:
                handle_explicit_header(
                    self.document, self.parent.children[-1], title)
            raise EOFError


class SpecializedBody(states.SpecializedBody, Body):
    """Instantiates the SpecializedBody class."""
    #: Define substitution
    substitution_def = states.SpecializedBody.invalid_input
    #: Add explicit markup
    explicit_markup = states.SpecializedBody.invalid_input
    #: Handle a figure
    figure = states.SpecializedBody.invalid_input
    #: Set the title
    title = states.SpecializedBody.invalid_input
    #: Handle a footnote
    footnote = states.SpecializedBody.invalid_input
    #: Set a literal block
    literal_block = states.SpecializedBody.invalid_input
    #: Set a block quote
    block_quote = states.SpecializedBody.invalid_input
    #: Set a table top
    markdown_table_top = states.SpecializedBody.invalid_input


class BulletList(states.BulletList, SpecializedBody):
    """Instantiates the BulletList class."""
    pass


class DefinitionList(states.DefinitionList, SpecializedBody):
    """Instantiates the DefinitionList class."""
    pass


class EnumeratedList(states.EnumeratedList, SpecializedBody):
    """Instantiates the EnumeratedList class."""

    def enumerator(self, match, context, next_state):
        """Enumerates a list item.

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two blank lists and the next state
        """
        format, sequence, text, ordinal = self.parse_enumerator(
            match, self.parent['enumtype'])
        if (format != self.format
                or (sequence != '#' and (sequence != self.parent['enumtype']
                                         or self.auto
                                         or ordinal != (self.lastordinal + 1)))
                or not self.is_enumerated_list_item(ordinal, sequence, format)):
            if sequence != '#' and ordinal != (self.lastordinal + 1):
                sequence = '#'
            else:
                # different enumeration: new list
                self.invalid_input()

        if sequence == '#':
            self.auto = 1

        listitem, blank_finish = self.list_item(match.end())
        self.parent += listitem
        self.blank_finish = blank_finish
        self.lastordinal = ordinal
        return [], next_state, []


class FieldList(states.FieldList, Body):
    """Instantiates the FieldList class."""
    pass


class ExtensionOptions(states.ExtensionOptions, Body):
    """Instantiates the ExtensionOptions class."""
    pass


class Explicit(states.Explicit, Body):
    """Instantiates the Explicit class."""
    pass


class Text(states.Text, Body):
    """Instantiates the Text class."""
    #: Dict of RE patterns
    patterns = {'bullet': Body.patterns['bullet'],
                'enumerator': Body.patterns['enumerator'],
                'text': r''}
    #: List of initial transitions
    initial_transitions = [
        ('bullet', 'Body'),
        ('enumerator', 'Body'),
        ('text', 'Body')
    ]

    pass


class SpecializedText(states.SpecializedText, Text):
    """Instantiates the SpecializedText class."""
    pass


class Definition(states.Definition, SpecializedText):
    """Instantiates the Definition class."""
    pass


class Line(states.Line, SpecializedText):
    """Instantiates the Line class."""
    pass


class Meta(Body):
    """Instantiates the Meta class."""
    #: Dict of RE patterns to match
    patterns = {
        'meta': Body.patterns['meta'],
        'option': r'^\s{0,3}([^:]+)\s*:\s*(.+)\s*$',
        'text': Body.patterns['text']
    }
    #: List of initial transitions
    initial_transitions = [
        ('meta', 'Body'),
        ('option', 'Meta'),
        ('text', 'Meta')
    ]

    def meta(self, match, context, next_state):
        """Handle a meta field

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two empty lists and the next state
        """
        return [], next_state, []

    def option(self, match, context, next_state):
        """Handle an option

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two empty lists and the next state
        """
        src, srcline = self.state_machine.get_source_and_line()
        meta = addnodes.meta(match.string)
        meta.source = src
        meta.line = srcline
        meta['name'] = match[1]
        meta['content'] = match[2]

        self.nested_parse(StringList(
            initlist=[match[2]]), input_offset=0, node=meta)

        self.parent += meta

        return [], next_state, []

    def text(self, match, context, next_state):
        """Handle text item

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two empty lists and the next state
        """
        srcline = self.state_machine.abs_line_number()
        msg = self.reporter.warning(
            'A non-recognized item in the metadata block',
            nodes.literal_block(match.string, match.string),
            line=srcline)
        self.parent += msg
        return [], next_state, []


class SubstitutionDef(states.SubstitutionDef, Body):
    """Instantiates the SubstitutionDef class."""
    #: Dict of RE patterns to match
    patterns = {
        'reference': r'^\[',
        'image': r'^!\[',
        'embedded_directive': re.compile(r'(%s)::( +|$)'
                                         % states.Inliner.simplename, re.UNICODE),
        'text': r''}
    #: List of initial transitions
    initial_transitions = ['reference', 'image', 'embedded_directive', 'text']

    def reference(self, match, context, next_state):
        """Handle a reference object

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two empty lists and the next state
        """
        self._inline_parse()
        return [], next_state, []

    def image(self, match, context, next_state):
        """Handle an image object

        :param self: Current instance
        :param match: RE match
        :param context: Current context
        :param next_state: Next state of the system
        :return: Two empty lists and the next state
        """
        self._inline_parse()
        return [], next_state, []

    def _inline_parse(self):
        """Parse an object inline

        :param self: Current instance
        """
        text = self.state_machine.line
        lineno = self.state_machine.abs_line_number() - 1
        nodes, messages = self.inline_text(text, lineno)
        self.parent += nodes
        self.parent += messages


class MarkdownInliner(states.Inliner):
    """Instantiates the MarkdownInliner class."""
    #: Add email
    emailc = states.Inliner.emailc
    #: End of url
    uri_end = states.Inliner.uri_end
    #: Main url
    uric = states.Inliner.uric
    #: Whitespace to be escaped
    non_unescaped_whitespace_escape_before = (
        states.Inliner.non_unescaped_whitespace_escape_before
    )
    #: Simple name of the document
    simplename = states.Inliner.simplename

    def init_customizations(self, settings):
        """Initialiaze customizations

        :param self: Current instance
        :param settings: Settings to initialize
        """
        start_string_prefix = r'(?:^|\s|\W)'
        end_string_suffix = ''

        args = locals().copy()
        args.update(vars(self.__class__))

        parts = ('initial_inline', start_string_prefix, '',
                 [
                     ('start', '', '',  # simple start-strings
                      [r'<!--\s*' + self.non_whitespace_after,  # comment
                       # strong or emphasis
                       r'\*{1,2}|_{1,2}' + self.non_whitespace_after,
                       r'``',  # literal_esc
                       r'`',  # literal
                       r'\<(?!\>)' + self.non_whitespace_after]  # substitution reference
                      ),
                     ('whole', '', end_string_suffix,  # whole constructs
                      [
                          # images
                          r'(?P<img_begin>\!\[)(?P<alt_text>[^\(\)\[\]]+)](?:\((?P<img_uri>[^\"\(\)\[\]]+)(?:\s+\"(?P<img_caption>.+)\")?\)|(?:\[(?P<img_id>[^\[\]\(\)]+)\]))',
                          # footnote references
                          r'(?P<footn>\[)(?P<footnotelabel>\d+|#|\*)\](?P<citationlabel>)',
                          # references
                          r'\[(?P<refname_uri>(?:\[[^^\]]*\]|[^\[\]]|\](?=[^\[]*\]))*)\]\((?P<refuri>[\s\S]*?)(?P<refend_uri>\))',
                          r'!?\[(?P<refname_id>(?:\[[^^\]]*\]|[^\[\]]|\](?=[^\[]*\]))*)\]\s*\[(?P<refid>[^^\]]*)(?P<refend_id>\])',
                          r'(?<!\]|\!)\[(?P<refname_text>[\w\s\d]+)(?P<refend_text>\])(?!\[|\(|\:)'
                      ]
                      )
                 ]
                 )

        self.patterns = states.Struct(
            initial=states.build_regexp(parts),
            emphasis1=re.compile(self.non_whitespace_escape_before
                                 + r'(\*)' + end_string_suffix, re.UNICODE),
            emphasis2=re.compile(self.non_whitespace_escape_before
                                 + r'(_)' + end_string_suffix, re.UNICODE),
            strong1=re.compile(self.non_whitespace_escape_before
                               + r'(\*\*)' + end_string_suffix, re.UNICODE),
            strong2=re.compile(self.non_whitespace_escape_before
                               + r'(__)' + end_string_suffix, re.UNICODE),
            literal=re.compile(r'(?<!`)(`)(?!`)'
                               + end_string_suffix, re.UNICODE),
            literal_esc=re.compile(r'(?<!`)(``)(?!`)'
                                   + end_string_suffix, re.UNICODE),
            email=re.compile(self.email_pattern % args + '$',
                             re.VERBOSE | re.UNICODE),
            uri=re.compile(
                (r"""
                        %(start_string_prefix)s
                        (?P<whole>
                          (?P<absolute>           # absolute URI
                            (?P<scheme>             # scheme (http, ftp, mailto)
                              [a-zA-Z][a-zA-Z0-9.+-]*
                            )
                            :
                            (
                              (                       # either:
                                (//?)?                  # hierarchical URI
                                %(uric)s*               # URI characters
                                %(uri_end)s             # final URI char
                              )
                              (                       # optional query
                                \?%(uric)s*
                                %(uri_end)s
                              )?
                              (                       # optional fragment
                                \#%(uric)s*
                                %(uri_end)s
                              )?
                            )
                          )
                        |                       # *OR*
                          (?P<email>              # email address
                            """ + self.email_pattern + r"""
                          )
                        )
                        %(end_string_suffix)s
                        """) % args, re.VERBOSE | re.UNICODE),
            substitution_ref=re.compile(self.non_whitespace_escape_before
                                        + r'(\>)'
                                        + end_string_suffix, re.UNICODE),
        )

        self.implicit_dispatch.append((self.patterns.uri,
                                       self.standalone_uri))

    def parse(self, text, lineno, memo, parent):
        """Return nodes (text and inline elements), and system_messages.

        Using `self.patterns.initial`, a pattern which matches start-strings
        (emphasis, strong, interpreted, phrase reference, literal,
        substitution reference, and inline target) and complete constructs
        (simple reference, footnote reference), search for a candidate.  When
        one is found, check for validity (e.g., not a quoted '*' character).
        If valid, search for the corresponding end string if applicable, and
        check it for validity.  If not found or invalid, generate a warning
        and ignore the start-string.  Implicit inline markup (e.g. standalone
        URIs) is found last.

        .. todo:: Needs to be refactored for nested inline markup.

        .. todo:: Add nested_parse() method?

        :param self: Current instance
        :param text: Text to parse
        :param lineno: Current line of document
        :param memo: Note about current line
        :param parent: The parent node
        :return nodes, system_messages: List of nodes and system messages
        """
        self.reporter = memo.reporter
        self.document = memo.document
        self.language = memo.language
        self.memo = memo
        self.parent = parent
        pattern_search = self.patterns.initial.search
        dispatch = self.dispatch
        remaining = escape2null(text)
        processed = []
        unprocessed = []
        messages = []
        while remaining:
            match = pattern_search(remaining)
            if match:
                groups = match.groupdict()
                method = dispatch[(groups['start'] or groups['footn'] or groups['refend_uri']
                                   or groups['refend_id'] or groups['refend_text'] or groups['img_begin']).strip()]
                before, inlines, remaining, sysmessages = method(self, match,
                                                                 lineno)
                unprocessed.append(before)
                messages += sysmessages
                if inlines:
                    processed += self.implicit_inline(''.join(unprocessed),
                                                      lineno)
                    processed += inlines
                    unprocessed = []
            else:
                break
        remaining = ''.join(unprocessed) + remaining
        if remaining:
            processed += self.implicit_inline(remaining, lineno)
        return processed, messages

    def _parse_ref_type(self, uri):
        """Parse types of references.

        :param self: Current instance
        :param uri: The uri of the current page
        :return typ: The type of reference
        """
        if '#' in uri:
            typ = 'md:ref'
        else:
            typ = None
            for ext in self.document.transformer.env.config.source_suffix:
                if uri.endswith(ext):
                    typ = 'md:doc'
                    break
        return typ

    def reference(self, match, lineno, anonymous=False):
        """Handle references in markdown.

        :param self: Current instance
        :param match: RE match
        :param lineno: Line number for current page
        :param anonymous: Is this an anonymous reference
        :return: Tuple containing a list of strings and the node
        """
        string = match.string
        matchstart = match.start('whole')
        matchend = match.end('whole')

        dict = match.groupdict()
        if dict['refname_uri']:
            name = match.group('refname_uri')
            uri = match.group('refuri')
            if self.patterns.uri.match(uri):
                # absolute uri
                node = nodes.reference(
                    string[matchstart:matchend], name,
                    name=nodes.whitespace_normalize_name(name))
                node['refuri'] = uri
            else:
                # relative uri
                typ = self._parse_ref_type(uri)
                if typ:
                    node = self.interpreted(string, name, typ, lineno)
                    node = node[0][0]
                else:
                    node = nodes.reference(
                        string[matchstart:matchend], name,
                        name=nodes.whitespace_normalize_name(name))
                    node['refuri'] = uri
        elif dict['refname_id']:
            name = match.group('refname_id')
            node = nodes.reference(
                string[matchstart:matchend], name,
                name=nodes.whitespace_normalize_name(name))
            node['refname'] = match.group('refid')
            node['names'] = [match.group('refid')]
            self.document.note_refname(node)
        elif dict['refname_text']:
            name = match.group('refname_text')
            node = nodes.reference(
                string[matchstart:matchend], name,
                name=nodes.whitespace_normalize_name(name))
            node['refname'] = name
            node['names'] = [name]
            self.document.note_refname(node)
        else:
            node = nodes.Text(string[matchstart:matchend], match.string)

        self._inspect_embedded_image(node, lineno)

        return (string[:matchstart], [node], string[matchend:], [])

    def _inspect_embedded_image(self, node, lineno):
        """Inspect an embedded image.

        Checks if reference contains embedded image
        and reformats node, if necessary

        :param self: Current instance
        :param node: Current node
        :param lineno: Current line number
        """
        text = node[0].astext()
        processed, messages = self.parse(
            text, lineno, self.memo, nodes.paragraph)
        if len(processed) == 1 and isinstance(processed[0], nodes.image):
            del node['name']
            node.remove(node[0])
            for child in processed:
                node.append(child)

    def image(self, match, lineno):
        """Handle a markdown image.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: Tuple of match start, string and match end.
        """
        string = match.string
        matchstart = match.start('whole')
        matchend = match.end('whole')

        dict = match.groupdict()

        node = nodes.image(string[matchstart:matchend])
        node['alt'] = dict['alt_text']
        if dict['img_uri']:
            node['uri'] = dict['img_uri']
        else:
            node.resolved = 0
            node['names'] = [dict['img_id']]
            node['refname'] = dict['img_id']
            self.document.note_refname(node)

        if dict['img_caption']:
            cap_begin = match.start('img_caption')
            cap_end = match.end('img_caption')
            caption = nodes.caption(
                string[cap_begin:cap_end], dict['img_caption'])
            node += caption

        return (string[:matchstart], [node], string[matchend:], [])

    def comment(self, match, lineno):
        """Handle markdown comment.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: previous line, inline edits, remaining lines, messages
        """
        before, inlines, remaining, sysmessages, endstring = self.inline_obj(
            match, lineno, re.compile(r'(?P<start>\s*-->)'), nodes.comment)
        return before, inlines, remaining, sysmessages

    def emphasis1(self, match, lineno):
        """Handle emphasis level one.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: previous line, inline edits, remaining lines, messages
        """
        before, inlines, remaining, sysmessages, endstring = self.inline_obj(
            match, lineno, self.patterns.emphasis1, nodes.emphasis, inline_parse=True)
        return before, inlines, remaining, sysmessages

    def emphasis2(self, match, lineno):
        """Handle emphasis level two.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: previous line, inline edits, remaining lines, messages
        """
        before, inlines, remaining, sysmessages, endstring = self.inline_obj(
            match, lineno, self.patterns.emphasis2, nodes.emphasis, inline_parse=True)
        return before, inlines, remaining, sysmessages

    def strong1(self, match, lineno):
        """Handle strong level one.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: previous line, inline edits, remaining lines, messages
        """
        before, inlines, remaining, sysmessages, endstring = self.inline_obj(
            match, lineno, self.patterns.strong1, nodes.strong, inline_parse=True)
        return before, inlines, remaining, sysmessages

    def strong2(self, match, lineno):
        """Handle strong level two.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: previous line, inline edits, remaining lines, messages
        """
        before, inlines, remaining, sysmessages, endstring = self.inline_obj(
            match, lineno, self.patterns.strong2, nodes.strong, inline_parse=True)
        return before, inlines, remaining, sysmessages

    def literal_esc(self, match, lineno):
        """Handle literal escaping.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: previous line, inline edits, remaining lines, messages
        """
        before, inlines, remaining, sysmessages, endstring = self.inline_obj(
            match, lineno, self.patterns.literal_esc, nodes.literal,
            restore_backslashes=True)
        return before, inlines, remaining, sysmessages

    def inline_obj(self, match, lineno, end_pattern,
                   nodeclass, restore_backslashes=False, inline_parse=False):
        """Handle inline objects.

        :param self: Current instance
        :param match: RE match
        :param lineno: Current line number
        :return: List of match start, nodes, matchend, and empty list
        and empty string
        """
        string = match.string
        matchstart = match.start('start')
        matchend = match.end('start')
        if self.quoted_start(match):
            return (string[:matchend], [], string[matchend:], [], '')
        endmatch = end_pattern.search(string[matchend:])
        if endmatch and endmatch.start(1):  # 1 or more chars
            _text = endmatch.string[:endmatch.start(1)]
            textend = matchend + endmatch.end(1)
            rawsource = unescape(string[matchstart:textend], True)
            if inline_parse:
                node = nodeclass(rawsource)
                children, messages = self.parse(_text, lineno, self.memo, node)
                node += children
            else:
                text = unescape(_text, restore_backslashes)
                node = nodeclass(rawsource, text)
                node[0].rawsource = unescape(_text, True)
                messages = []
            return (string[:matchstart], [node],
                    string[textend:], messages, endmatch.group(1))

        text = unescape(string[matchstart:matchend], True)
        rawsource = unescape(string[matchstart:matchend], True)

        return string[:matchstart], [nodes.Text(text, rawsource)], string[matchend:], [], ''

    #: Dispatch dict
    dispatch = {'<!--': comment,
                '*': emphasis1,
                '_': emphasis2,
                '**': strong1,
                '__': strong2,
                '`': states.Inliner.literal,
                '``': literal_esc,
                ')': reference,
                ']': reference,
                '<': states.Inliner.substitution_reference,
                '![': image,
                '[': states.Inliner.footnote_reference
                }


#: List of possible state classes.
state_classes = [
    Body,
    ExtensionOptions,
    BulletList,
    DefinitionList,
    EnumeratedList,
    FieldList,
    Explicit,
    Text,
    Definition,
    Line,
    Meta,
    SubstitutionDef
]
