"""Input markdown files for transformation."""
from docutils.statemachine import string2lines, StringList
from sphinx.io import SphinxRSTFileInput, SphinxBaseFileInput
import re

re_comment_start = re.compile(r'<!--')
re_comment_end = re.compile(r'-->')


def _build_contents(lines, path):
    """Build contents of the markdown file

    .. todo:: This is too complicated.

    :param lines: Lines of the file to edit.
    :param path: Path of the file to write.
    """
    content = StringList()

    line_no = 0
    while line_no < len(lines):
        readable_line_no = line_no + 1
        if re_comment_start.search(lines[line_no]) and not re_comment_end.search(lines[line_no]):
            line = lines[line_no]
            line_no += 1

            comment_ended = False
            while line_no < len(lines):
                line += '\n' + lines[line_no]
                if re_comment_end.search(lines[line_no]):
                    comment_ended = True
                    break
                line_no += 1

            if not comment_ended:
                line += '-->'
        else:
            line = lines[line_no]

        line_no += 1
        content.append(line, path, readable_line_no)

    return content


class MarkdownFileInput(SphinxRSTFileInput):
    """Instantiates the MarkdownFileInput class."""
    #: Supported languages list.
    supported = ['markdown']

    def read(self):
        """Read the markdown file for input.

        :param self: The current instance.
        :return list content: The list of strings to edit.
        """
        # type: () -> StringList
        inputstring = SphinxBaseFileInput.read(self)
        lines = string2lines(inputstring, convert_whitespace=True)
        content = _build_contents(lines, self.source_path)

        if self.env.config.rst_prolog:
            self.prepend_prolog(content, self.env.config.rst_prolog)
        if self.env.config.rst_epilog:
            self.append_epilog(content, self.env.config.rst_epilog)

        return content
