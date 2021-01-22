#
# Copyright (C) 2009-2020 the sqlparse authors and contributors
# <see AUTHORS file>
#
# This module is part of python-sqlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

"""filter"""

from sqlparse import lexer
from sqlparse.engine import grouping
from sqlparse.engine.statement_splitter import StatementSplitter


class FilterStack:
    def __init__(self):
        self.preprocess = []
        self.stmtprocess = []
        self.postprocess = []
        self._grouping = False

    def enable_grouping(self):
        self._grouping = True

    def run(self, sql, encoding=None, stream=False):
        context = {}

        stream = lexer.tokenize(sql, encoding, stream, context)
        # Process token stream
        for filter_ in self.preprocess:
            stream = filter_.process(stream)

        stream = StatementSplitter().process(stream)

        # Output: Stream processed Statements
        for stmt in stream:
            if 'mode' in context:
                del context['mode']

            first_token = stmt.token_first(True, True)
            if first_token and first_token.normalized == 'COPY':
                from_token = stmt.token_matching(
                    lambda t: t.normalized == 'FROM', 0)
                if from_token:
                    stdin_token = stmt.token_matching(
                        lambda t: t.is_keyword and t.normalized == 'STDIN',
                        from_token.parent.token_index(from_token))
                    if stdin_token:
                        context['mode'] = 'data'

            if self._grouping:
                stmt = grouping.group(stmt)

            for filter_ in self.stmtprocess:
                filter_.process(stmt)

            for filter_ in self.postprocess:
                stmt = filter_.process(stmt)

            yield stmt
