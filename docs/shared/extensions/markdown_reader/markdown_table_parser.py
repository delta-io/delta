"""Contains the MarkdownTableParser class."""
from docutils.parsers.rst.tableparser import TableMarkupError
from sphinx.parsers import StringList


class MarkdownTableParser:
    """Instantiate the MarkdownTableParser class."""

    def parse(self, block):
        """Parse markdown tables.

        :param self: Current instance.
        :param block: Block containing the table.
        :return colwidths, headros, bodyrows: Return the table.
        """
        header = self._parse_row(block[0])
        self._ensure_header_separator(block[1], len(header))
        rows = [self._parse_row(block[i]) for i in range(2, len(block))]
        colwidths = self._determine_colwidths(header, rows)
        headrows, bodyrows = self._prepare_table_spec(header, rows)
        return colwidths, headrows, bodyrows

    def _determine_colwidths(self, header, rows):
        """Find out the width of the table columns.

        :param self: Current instance.
        :param header: The current header.
        :param rows: The rows to be measured.
        :return wdiths: List of the widths of rows.
        """
        widths = [len(cell[0].strip()) for cell in header]
        for row in rows:
            for i_cell in range(0, len(row)):
                cell = row[i_cell]
                for line in cell:
                    width = len(line.strip())
                    if widths[i_cell] < width:
                        widths[i_cell] = width
        return widths

    def _parse_row(self, line):
        """Parse a row from a table.

        :param self: Current instance.
        :param line: Current line of text.
        :return titles: Titles of the rows.
        """
        raw_titles = line.strip('|').split('|')
        titles = [title.split('<br>') for title in raw_titles]
        return titles

    def _ensure_header_separator(self, line, expected_count):
        """Add header separator to table columns.

        :param self: Current instance.
        :param line: Line to be edited.
        :param expected_count: Number of columns to separate.
        """
        cols = line.strip('|').split('|')
        if len(cols) != expected_count:
            raise TableMarkupError(offset=1)

    def _prepare_table_spec(self, header, rows):
        """Prepare table spec.

        :param self: Current instance.
        :param header: Header of the current table.
        :param rows: List of rows.
        :return headrows, bodyrows: Header rows and body rows of the table.
        """
        headrows = [
            [(0, 0, 0, StringList([line.strip() for line in cell])) for cell in
             header]]  # format: (morerows, morecols, line_offset, cell_text)
        bodyrows = []
        for i in range(0, len(rows)):
            line_offset = i + 2
            bodyrows.append(
                [(0, 0, line_offset, StringList(
                    [line for line in cell])) for cell in rows[i]])
        return headrows, bodyrows
