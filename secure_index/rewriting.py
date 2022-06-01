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

import functools
from collections import defaultdict

import sqlparse
import sqlparse.sql as S
import sqlparse.tokens as T


if __package__:
    from .sqlparser import parse
else:
    from secure_index.sqlparser import parse

def truncate(state):
    # Filter out group by, having and order by
    if state.other is not None:
        state.tokens = state.tokens[:state.other]


def rewrite_projection(state):
    start, end = state.projection
    for _ in range(start, end):
        del state.tokens[start]
    for i, token in enumerate(sqlparse.parse(" \"EncTuples\" ")[0].tokens):
        state.tokens.insert(start + i, token)


def rewrite_table_with_mapping(state):
    rewritten = f"{state.table.normalized} JOIN mapping USING (\"GroupId\")"
    state.table.tokens = sqlparse.parse(rewritten)[0].tokens


def filter(tokens):
    """Filter out whitespaces and comments.
    
    :tokens: List of tokens to filter.
    :return: List of tokens without whitespaces and comments.
    """
    filtered = []
    for tok in tokens:
        if not (tok.is_whitespace or isinstance(tok, S.Comment) or \
                tok is T.Comment.Single):
            filtered.append(tok)
    return filtered


def drop_double_quotes(string):
    if string.startswith('"') and string.endswith('"'):
        return string[1:-1]
    return string


def get_column(string):
    if string.find('.') != -1:
        splits = string.split('.')
        if len(splits) != 2:
            raise Exception(
                "Column identifiers may contain only one dot separating " +
                "table name and column name."
            )
        _, string = splits
    return drop_double_quotes(string)


def rewrite_table_with_normalization(state):
    columns = set()
    for comparison in state.comparisons:
        left, op, right, *additional = filter(comparison.tokens)

        if isinstance(left, S.Identifier) and isinstance(right, S.Identifier):
            raise Exception("Comparisons among two columns are not supported.")

        # Swap column identifier to the left
        if isinstance(right, S.Identifier):
            left, right = right, left

        column = get_column(left.normalized)
        columns.add(column)


    # Rewrite query according to the columns in the comparisons
    rewritten = [state.table.normalized,
               "JOIN \"GroupIdToColumns\" USING (\"GroupId\")"]
    for column in columns:
        rewritten.append(f"JOIN \"{column}\" ON (\"{column}Id\" = \"{column}\".\"Id\")")
    state.table.tokens = sqlparse.parse(" ".join(rewritten))[0].tokens


def get_number(string):
    num = float(string)
    if num.is_integer():
        num = int(num)
    return num


def get_numbers(string):
    if string.startswith('(') and string.endswith(')'):
        return set(map(get_number, string[1:-1].split(',')))
    return None


def get_extremes(a, b):
    return get_number(a), get_number(b)


def to_string(labels):
    if isinstance(next(iter(labels)), str):
        return "'" + "'),('".join(sorted(labels)) + "'"
    return "),(".join(map(str, sorted(labels)))


ROTATE = {"=": "=", ">": "<", "<": ">", ">=": "<=", "<=": ">=", "<>": "<>"}

def rewrite_comparisons(mapping, state, kv_store_data=None):
    FUNCTIONS = {
        "=": mapping.eq,  ">": mapping.gt, "<": mapping.lt, ">=": mapping.ge,
        "<=": mapping.le, "<>": mapping.neq, "in": mapping.in_values,
        "between": mapping.between
    }

    for comparison in state.comparisons:
        left, op, right, *additional = filter(comparison.tokens)
        if len(additional):
            additional = additional[0]

        if isinstance(left, S.Identifier) and isinstance(right, S.Identifier):
            raise Exception("Comparisons among two columns are not supported.")

        # Swap column identifier to the left
        if isinstance(right, S.Identifier):
            left, right = right, left
            op = S.Token(T.Operator.Comparison, ROTATE[op.normalized])

        try:
            # Retrieve the list of labels based on operator and value
            column = get_column(left.normalized)
            function = FUNCTIONS[op.normalized.lower()]
            get_param = get_number
            if function == FUNCTIONS["in"]:
                get_param = get_numbers
            elif function == FUNCTIONS["between"]:
                get_param = functools.partial(get_extremes,
                                              b=additional.normalized)
            labels = function(column, get_param(right.normalized))

            # Rewrite comparison
            rewritten = "False"
            if labels:
                column = column if not mapping.is_gid(column) else "GroupId"

                if kv_store_data is not None:
                    # Populate a dictionary with the requested labels
                    # NOTE: to improve query performance we assume that
                    # multiple conditions on the same column are in AND
                    if not kv_store_data[column]:
                        kv_store_data[column].update(labels)
                    else:
                        kv_store_data[column].intersection_update(labels)
                    continue
                
                rewritten = '"' + column + "\" IN (VALUES (" + to_string(labels) + "))"

            comparison.tokens = sqlparse.parse(rewritten)[0].tokens

        except KeyError:
            raise Exception(
                f"{op.normalized} is not supported as a comparison operator."
            )
        except ValueError:
            raise Exception(
                f"{right.normalized} is not numeric. " +
                 "Range mapping does not support strings yet."
            )


def rewrite(query,
            mapping,
            rewrite_table=None,
            rewrite_comparisons=rewrite_comparisons,
            kv_store_mode=False):
    """
    :kv_store_mode: removes part of the query rewriter functionality of the rewriter
    """
    state = parse(query)
    truncate(state)

    rewrite_projection(state)
    if rewrite_table is not None:
        rewrite_table(state)

    kv_store_data = defaultdict(set) if kv_store_mode else None
    rewrite_comparisons(mapping, state, kv_store_data)

    table = drop_double_quotes(state.table.normalized)

    if kv_store_mode:
        return kv_store_data, table

    rewritten = str(state)
    return rewritten, table
