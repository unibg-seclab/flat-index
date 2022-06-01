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

if __package__:
    from .interface import Mapping
    from .runtime_token_to_representation import get_token_representations
    from .runtime_token_to_representation import get_all_tokens_representations    
else:
    from secure_index.mapping._column_mapping.interface import Mapping
    from secure_index.mapping._column_mapping.runtime_token_to_representation import get_token_representations
    from secure_index.mapping._column_mapping.runtime_token_to_representation import get_all_tokens_representations

START = 0
END = 1


class RangeMapping(Mapping):
    """Range mapping.
    
    :tokens: List of tokens associated with each range.
    :ranges: List of ranges ordered by left extreme.
    :by_end: List of positions ordering ranges by right extreme.
    :is_runtime: Indicate whether tokens should be generated at runtime.
    :key: Key to generate the tokens.
    :salt: Random salt to generate the tokens.
    """

    def __init__(self, data):
        self.tokens, self.ranges, self.by_end, self.is_runtime, self.key, self.salt = data

    def _get_tokens(self, token):
        if self.is_runtime:
            return get_token_representations(token, self.key, self.salt)
        return token

    def get_generalizations(self):
        generalizations = [None] * len(self.ranges)
        for i, _range in enumerate(self.ranges):
            if _range[START] != _range[END]:
                generalizations[i] = f"[{_range[START]}-{_range[END]}]"
            else:
                generalizations[i] = str(_range[START])
        return generalizations

    def get_tokens(self):
        if self.is_runtime:
            return get_all_tokens_representations(self.tokens, self.key, self.salt)
        return self.tokens

    def between(self, extremes):
        a,b = extremes
        return self.ge(a) & self.le(b)

    def eq(self, value):
        return self.between((value, value))

    def neq(self, value):
        return self.lt(value) | self.gt(value)

    def ge(self, value):
        after = set()
        for i in self.by_end:
            if self.ranges[i][END] < value:
                break
            after.update(self._get_tokens(self.tokens[i]))
        return after

    def gt(self, value):
        after = set()
        for i in self.by_end:
            if self.ranges[i][END] <= value:
                break
            after.update(self._get_tokens(self.tokens[i]))
        return after

    def le(self, value):
        before = set()
        for i, _range in enumerate(self.ranges):
            if _range[START] > value:
                break
            before.update(self._get_tokens(self.tokens[i]))
        return before

    def lt(self, value):
        before = set()
        for i, _range in enumerate(self.ranges):
            if _range[START] >= value:
                break
            before.update(self._get_tokens(self.tokens[i]))
        return before
