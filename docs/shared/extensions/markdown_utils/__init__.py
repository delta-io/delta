"""Contains markdown utilities."""
from os import path
import re

from docutils import nodes


class MarkdownTableFormatter:
    """Instantiates the MarkdownTableFormatter class."""

    def __init__(self, tablestring):
        """Initialize a MarkdownTableFormatter object.

        :param self: Current instance.
        :param tablestring: The table in string format.
        """
        self._rows = [self._split_row(row) for row in tablestring.splitlines()]
        self._col_widths = [max(map(len, column))
                            for column in zip(*self._rows)]

    def output(self):
        """Beautifies the text representation of a markdown table.

        :param self: Current instance.
        :return str out: Formatted output.
        """
        out = []
        for row in self._rows:
            for entry, width in zip(row, self._col_widths):
                if not width:
                    continue
                if entry.startswith('-') and entry.endswith('-'):
                    out.append("|-{}-".format('-' * width))
                else:
                    out.append("| {} ".format(str.ljust(entry, width)))
            out.append("|\n")

        return ''.join(out)

    def _split_row(self, row):
        """Split a row into its constituent parts.

        :param self: Current instance.
        :param row: The row to be split.
        """
        cells = []
        if '|-' in row:
            cells = [cell.strip() for cell in row.split('|')]
            for index in range(0, len(cells)-1):
                if cells[index] == '':
                    del cells[index]
        elif 'expr1 |' in row:
            match = re.match((
                r'^\|\s(\[.*\][a-z\/\(\)\.]*)\s+\|\s([a-z0-9`].*)\|(.*)\|'),
                row)
            if match is not None:
                cells = [match.group(1).strip(),
                         match.group(2).strip(),
                         match.group(3).strip()]
            else:
                match = re.match(
                    r'^.*?\|([a-z0-9\(\)\/\.\ \[\]\|]*)\|(.*)\|',
                    row)
                cells = [match.group(1).strip(),
                         match.group(2).strip()]
        elif 'H | L' in row:
            match = re.match(
                r'^\|\s(.*\))\|(.*)\|',
                row
            )
            cells = [match.group(1).strip(),
                     match.group(2).strip()]
        elif '|`' in row:
            match = re.match(
                r'^\|\s(.*?)\|\s(.*)\|',
                row
            )
            cells = [match.group(1),
                     match.group(2)]
        else:
            cells = [cell.strip() for cell in row.split('| ')]
            if cells[0] == '':
                del cells[0]
            cells[-1] = cells[-1].replace('|', '')
        return cells


def _clean_existing_targets(document, node):
    """Clean existing targets for transformation.

    :param document: Document to be operated on.
    :param node: Current node in the document.
    """
    for name in node['names']:
        del document.nameids[name]
        del document.nametypes[name]
    node['names'] = []
    node['ids'] = []


def _resolve_name_conflicts(document, name):
    """Resolve name conflicts.

    :param document: Document to be operated on.
    :param name: Name of the document to be operatd on.
    :return resolved_name: Return the resolved document name.
    """
    resolved_name = name
    index = 1
    while resolved_name in document.nameids:
        resolved_name = name + '-' + str(index)
        index += 1
    return resolved_name


#: Target file separator.
target_file_sep = '--'


def make_target_name(document, title):
    """Make a target's name.

    :param document: Document to be operated on.
    :param title: Title of the document.
    :return name: The name of the target.
    """
    doc_dir = document.transformer.env.srcdir + path.sep
    name = nodes.make_id(
        document['source'][len(doc_dir):] + target_file_sep + title)
    return name


def handle_explicit_header(document, section_node, title):
    """Handle explicit header.

    :param document: Document to operate on.
    :param section_node: Node in the current section.
    :param title: Title of the current node.
    """
    _clean_existing_targets(document, section_node)
    name = make_target_name(document, title)
    if not name.endswith(target_file_sep):
        name = _resolve_name_conflicts(document, name)
        section_node['names'].append(name)
        document.note_explicit_target(section_node, section_node)
