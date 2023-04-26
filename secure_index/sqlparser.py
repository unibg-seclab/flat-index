# Copyright 2022 Unibg Seclab (https://seclab.unibg.it)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sqlparse
import sqlparse.sql as S
import sqlparse.tokens as T


DEBUG = False


class State:
    """Represents some query structure information.

    :tokens: List of tokens obtained by parsing the query.
    :projection: List of tokens representing the projection.
    :table: Token of the table identifier.
    :comparisons: List of tokens representing comparisons in the where clause.
    :other: Position of the first occurance of a group by, having or order by
        clause.
    :comparisons: List of comparisons the query uses as selection.
    """

    def __init__(self, tokens):
        self.tokens = tokens
        self.projection = None
        self.table = None
        self.comparisons = []
        self.other = None

    def __str__(self):
        return ''.join(str(token) for token in self.tokens)


def parse(stmt):
    """Parse the SQL statement and return table name and a set of conditions.

    :statement: The SQL statement to parse.
    :return: Table name and a set of conditions.
    """
    stmt = sqlparse.parse(stmt)[0]

    if not stmt.tokens:
        raise Exception("Empty SQL statement provided")

    return statement(stmt.tokens)


def _first(tokens):
    """Search for the first token not being a whitespace or comment.

    :tokens: The list of tokens to search.
    :return: The index of the first token not being a whitespace or comment.
        -1 if no such token exists.
    """
    i = 0
    while i < len(tokens) and (tokens[i].is_whitespace or isinstance(
            tokens[i], S.Comment) or tokens[i].ttype is T.Comment.Single):
        i += 1

    if i == len(tokens):
        return -1
    return i


def _token_first(tokens):
    """Search for the first token not being a whitespace or comment.

    :tokens: The list of tokens to search.
    :return: The first token not being a whitespace or comment. `None` if
        no such token exists.
    """
    idx = _first(tokens)
    return tokens[idx] if idx != -1 else None


def statement(tokens):
    """Identify the statement and calls the proper handlers.

    :tokens: The sqlparse statement representation.
    :return: A tree storing the tables and columns identifiers the statement
        uses.
    """
    first = _token_first(tokens)
    if first and first.match(T.Keyword.DML, ["INSERT", "UPDATE", "DELETE"]):
        raise Exception(f"'{first.normalized}' is not supported yet.")
    if first and first.match(T.Keyword.DML, "SELECT"):
        return select(tokens)
    return None

def select(tokens):
    """Visits the SELECT statement and creates its state representation.

    The SELECT statements comply to the following syntax:

        SELECT [DISTINCT|ALL] expression+
        FROM table
        [WHERE expression]
        [GROUP BY expression+ [HAVING expression]]
        [ORDER BY (expression [ASC|DESC] [NULLS [FIRST|LAST]])+]

    :tokens: The sqlparse SELECT statement representation.
    :return: A tree storing the tables and columns identifiers the statement
        uses.
    """
    state = State(tokens)

    i = 0
    while i < len(tokens):
        tok = tokens[i]

        if DEBUG:
            print(type(tok).__name__, tok.ttype, tok)

        # SELECT [DISTINCT|ALL] expression+
        if tok.match(T.Keyword.DML, ["SELECT"]):
            length = _ignore_comma_separated_list(
                tokens[i + 1:],
                skip=_skip_modifier,
                limiter=lambda tok: tok.match(T.Keyword, "FROM"))
            state.projection = (i + 1, i + length + 1)
            i += length

        # FROM table
        elif tok.match(T.Keyword, "FROM"):
            length = _comma_separated_list(
                state,
                tokens[i + 1:],
                item_resolver=_table_resolver,
                limiter=lambda tok: isinstance(tok, S.Where) or tok.match(
                    T.Keyword, ["GROUP BY", "ORDER BY"]))
            i += length

        # [WHERE expression]
        elif isinstance(tok, S.Where):
            if DEBUG:
                print("WHERE")
            # TODO: store information on the expression
            _expression(state, tok.tokens[1:])

        # [GROUP BY expression+ [HAVING expression]]
        # [ORDER BY (expression [ASC|DESC] [NULLS [FIRST|LAST]])+]
        elif tok.match(T.Keyword, ["GROUP BY", "HAVING", "ORDER BY"]):
            state.other = i
            break

        elif tok.is_keyword:
            raise Exception(
                f"""Unexpected keyword '{tok.normalized}'. This may be an invalid or
                    unsupported keyword.
                    To use a keyword as a plain string, use backticks:
                    `{tok.normalized}`
                """)

        i += 1

    return state


def _ignore_comma_separated_list(tokens,
                                 skip=None,
                                 limiter=None):
    """Ignore comma separate list of items.

    :tokens: The list of tokens representing the comma separated list or part
        of it.
    :skip: A function that skips tokens at the beginning of tokens and return
        the number of tokens skipped.
    :limiter: A function that identifies tokens representing the end of the
        comma separated list.
    :return: The number of tokens part of the comma separated list.
    """
    skipped = 0
    if skip:
        skipped = skip(tokens)
        tokens = tokens[skipped:]

    # consider tokens till the limiter
    if limiter:
        limit = 0
        for tok in tokens:
            if limiter(tok):
                break
            limit += 1
        tokens = tokens[:limit]

    return skipped + len(tokens)


def _skip_modifier(tokens):
    """Skip DISTINCT and ALL modifiers.

    :tokens: The list of tokens representing the comma separated list or part
        of it.
    :return: The number of tokens skipped.
    """
    idx = _first(tokens)

    if idx == -1:
        return 0

    modifier = tokens[idx].match(T.Keyword, ["DISTINCT", "ALL"])
    return idx + modifier


def _comma_separated_list(state,
                          tokens,
                          item_resolver,
                          skip=None,
                          limiter=None):
    """Resolve comma separate list of items.

    :state: The current state on which the function operates.
    :tokens: The list of tokens representing the comma separated list or part
        of it.
    :item_resolver: A function that takes care of resolving a single list
        element.
    :skip: A function that skips tokens at the beginning of tokens and return
        the number of tokens skipped.
    :limiter: A function that identifies tokens representing the end of the
        comma separated list.
    :return: The number of tokens part of the comma separated list.
    """
    skipped = 0
    if skip:
        skipped = skip(tokens)
        tokens = tokens[skipped:]

    # consider tokens till the limiter
    if limiter:
        limit = 0
        for tok in tokens:
            if limiter(tok):
                break
            limit += 1
        tokens = tokens[:limit]

    length = skipped + len(tokens)

    # expand the tokens wrapped in IdentifierList
    expansion = []
    for tok in tokens:
        if isinstance(tok, S.IdentifierList):
            # hoping sqlparse does not wrap IdentifierList in other
            # IdentifierList
            expansion.extend(tok.tokens)
        else:
            expansion.append(tok)
    tokens = expansion

    # separate the list of tokens based on commas
    params = []
    current = []
    for tok in tokens:

        if DEBUG:
            print(type(tok).__name__, tok.ttype, tok)

        if tok.match(T.Punctuation, ','):
            if not current:
                raise Exception("invalid syntax: comma")
            params.append(current)
            current = []
        else:
            current.append(tok)
    if not current:
        raise Exception("invalid syntax: comma")
    params.append(current)

    # resolve parameters
    for param in params:
        item_resolver(state, param)
    return length


def _table_resolver(state, param):
    """Resolve a table expression.

    :state: The state on which the function operates.
    :param: The list of tokens representing the parameter of a comma separated
        list.
    """
    def _resolve_table(state, token):
        if isinstance(token, S.Identifier):
            if token.has_alias():
                raise Exception("Alias is not supported yet.")
            state.table = token
            return True
        return False

    if len(param) == 1:
        tok = param[0]
        _resolve_table(state, tok)
    else:
        i = 0
        while i < len(param):
            tok = param[i]

            table = _resolve_table(state, tok)
            if not table and \
                    tok.match(T.Keyword, ["CROSS JOIN",
                                          "FULL OUTER JOIN",
                                          "LEFT OUTER JOIN",
                                          "RIGHT OUTER JOIN",
                                          "JOIN",
                                          "INNER JOIN",
                                          "ON"]):
                raise Exception(f"{tok.normalized} is not supported yet.")

            i += 1


UNARY = [
    "NOT", "ALL", "ANY", "EXISTS", "SOME"   # logical
]

BINARY = [
    "AND", "IN", "LIKE", "OR",              # logical
    "=", ">", "<", ">=", "<=", "<>",        # comparison
    "+", "-", "*", "/", "%",                # arithmetic
    "||",                                   # string
    "&", "|", "^",                          # bitwise
    "IS"                                    # NULL values
]

TERNARY = [("BETWEEN", "AND")]

PRECEDENCE = {
    "+": 5, "-": 5, "*": 5, "/": 5, "%": 5, "&": 5, "|": 5, "^": 5, "||": 5,
    "ALL": 5, "ANY": 5, "EXISTS": 5, "IN": 5, "SOME": 5,
    "=": 4, ">": 4, "<": 4, ">=": 4, "<=": 4, "<>": 4,
    "BETWEEN": 3, "LIKE": 3,
    "NOT": 2,
    "AND": 1,
    "OR": 0,
    "IS": -1
}

OPERATORS = list(PRECEDENCE.keys())


# TODO: support rewrite of IN and BETWEEN statements
# TODO: handle sqlparse issue #370 on '-' arithmetic operator
def _expression(state, tokens):
    """Resolve a SQL expression.

    :state: The state on which the function operates.
    :tokens: The list of tokens containing the expression.
    :return: The number of tokens representing the expression.
    """
    # stacks for a shift-reduce parser
    args = []
    ops = []

    def _shift(val, args, pos=None):
        item = (val, pos) if pos is not None else val
        args.append(item)

    def _reduce(args, ops):
        assert len(ops) >= 1
        op_name = ops.pop()

        # ternary operators
        if ops and (ops[-1], op_name) in TERNARY:
            assert len(args) >= 3
            op = ops.pop()
            right, end = args.pop()
            middle, _ = args.pop()
            left, start = args.pop()

            # Treat "... BETWEEN ... AND ..." as a comparison
            string_to_parse = f"{left} BETWEEN {middle} AND {right}"
            comparison = sqlparse.parse(string_to_parse)[0]
            state.comparisons.append(comparison)
        # binary operators
        elif op_name in BINARY:
            assert len(args) >= 2
            right, end = args.pop()
            left, start = args.pop()

            # Treat "... IN ..." as a comparison
            if op_name == "IN":
                comparison = sqlparse.parse(f"{left} IN {right}")[0]
                state.comparisons.append(comparison)
        # unary operators
        elif op_name in UNARY:
            assert len(args) >= 1
            args.pop()  # arg
        else:
            print(f"Unexpected keyword '{op_name}'.")
        args.append("placeholder")

    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if DEBUG:
            print(tok, args, ops)

        # sqlparse packages up parenthesis
        if isinstance(tok, S.Parenthesis):
            subtokens = tok.tokens[1:-1]
            first = _token_first(subtokens)
            if first and first.match(T.Keyword.DML, "SELECT"):
                raise Exception("Subqueries are not supported yet.")
            
            _comma_separated_list(state, subtokens, item_resolver=_expression)
            _shift(tok.normalized, args, i)

        # sqlparse packages up comparisons
        elif isinstance(tok, S.Comparison):
            state.comparisons.append(tok)
            _expression(state, tok.tokens)
            _shift("comparison", args, i)

        # sqlparse packages up arithmetic and bitwise operations
        elif isinstance(tok, S.Operation):
            raise Exception("Operations are not supported.")

        # sqlparse packages up functions
        elif isinstance(tok, S.Function):
            raise Exception("Functions are not supported.")

        # sqlparse packages up cases
        elif isinstance(tok, S.Case):
            raise Exception("Case is not supported.")

        # resolve operator
        elif tok.match(T.Keyword, [keyword for _, keyword in TERNARY]) and \
                ops and (ops[-1], tok.normalized) in TERNARY:
            _shift(tok.normalized, ops)
        elif tok.match(T.Keyword, OPERATORS) or \
                tok.match(T.Operator, OPERATORS) or \
                tok.match(T.Operator.Comparison, OPERATORS):
            while ops and PRECEDENCE[ops[-1]] >= PRECEDENCE[tok.normalized]:
                _reduce(args, ops)
            _shift(tok.normalized, ops)

        # name or something with an alias
        elif isinstance(tok, S.Identifier):
            # sqlparse treats anything with an alias as an identifier
            if tok.has_alias():
                raise Exception("invalid syntax: alias")

            # sqlparse treats string literals as identifiers
            is_string_literal = tok.normalized.startswith('"') and \
                    tok.normalized.endswith('"')
            if not is_string_literal:
                # remove ordering information
                if tok.get_ordering():
                    tok = tok.tokens[0]
            _shift(tok.normalized, args, i)

        # literal
        elif tok.match(T.Keyword, ["^NULL$", "^NOT\\s+NULL$"], regex=True) or \
                tok.ttype in [T.Literal,
                              T.Literal.Number,
                              T.Literal.String,
                              T.Literal.String.Single,
                              T.Number,
                              T.Number.Float,
                              T.Number.Integer,
                              T.String,
                              T.String.Symbol]:
            _shift(tok.normalized, args, i)

        # whitespaces and comments
        elif tok.is_whitespace or isinstance(tok, S.Comment) or \
                tok is T.Comment.Single:
            pass

        else:
            break

        i += 1

    while ops and len(args) >= 1:
        _reduce(args, ops)

    if len(args) != 1:
        raise Exception("invalid comparison clause: %s" % tokens)


if __name__ == "__main__":
    print("[*] SELECT COUNT(*) FROM wrapped WHERE AGE<=18")
    print(parse("SELECT COUNT(*) FROM wrapped WHERE AGE<=18"))
    print("[*] SELECT COUNT(*) FROM wrapped WHERE AGE<=18 AND STATEFIP=55")
    print(parse("SELECT COUNT(*) FROM wrapped WHERE AGE<=18 AND STATEFIP=55"))
    print("[*] SELECT COUNT(*) FROM wrapped WHERE AGE<=18 AND STATEFIP=55 OR OCC IN (6260, 4700)")
    print(parse("SELECT COUNT(*) FROM wrapped WHERE AGE<=18 AND STATEFIP=55 OR OCC IN (6260, 4700)"))
    print("[*] SELECT COUNT(*) FROM wrapped WHERE AGE<=18 AND (STATEFIP=55 OR OCC IN (6260, 4700))")
    print(parse("SELECT COUNT(*) FROM wrapped WHERE AGE<=18 AND (STATEFIP=55 OR OCC IN (6260, 4700))"))
