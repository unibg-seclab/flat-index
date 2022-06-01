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

    
DELTA = 1


class IntervalTreeMapping(Mapping):
    """Interval tree mapping.
    
    :interval_tree: Interval tree storing tokens associated with each range.
    :is_runtime: Indicate whether tokens should be generated at runtime.
    :key: Key to generate the tokens.
    :salt: Random salt to generate the tokens.
    """

    def __init__(self, data):
        self.interval_tree, self.is_runtime, self.key, self.salt = data

    def _get_tokens(self, token):
        if self.is_runtime:
            return get_token_representations(token, self.key, self.salt)
        return token

    def get_generalizations(self):
        generalizations = [None] * len(self.interval_tree[:])
        for i, _interval in enumerate(self.interval_tree[:]):
            _range_start = _interval[0]
            _range_end = _interval[1] - DELTA
            # please note that point are generalized as "fake" intervals
            # [point, point+1)
            if _range_start != _range_end:
                generalizations[i] = f"[{_range_start}-{_range_end}]"
            else:
                generalizations[i] = str(_range_start)
        return generalizations

    def get_tokens(self):
        tokens = [None] * len(self.interval_tree[:])
        for i, _interval in enumerate(self.interval_tree[:]):
            tokens[i] = _interval[2]
        # return a list of list elements either case
        if self.is_runtime:
            return get_all_tokens_representations(tokens, self.key, self.salt)
        return tokens

    def between(self, extremes):
        a,b = extremes
        return {
            token
            for interval in self.interval_tree[a:b + DELTA]
            for token in self._get_tokens(interval[2])
        }

    def eq(self, value):
        return {
            token
            for interval in self.interval_tree[value]
            for token in self._get_tokens(interval[2])
        }

    def neq(self, value):
        # TODO: support this by excluding [value, value+DELTA)
        return {token for tokens in self.get_tokens() for token in tokens}

    def ge(self, value):
        return {
            token
            for interval in self.interval_tree[value:]
            for token in self._get_tokens(interval[2])
        }

    def gt(self, value):
        return {
            token
            for interval in self.interval_tree[value + DELTA:]
            for token in self._get_tokens(interval[2])
        }

    def le(self, value):
        return {
            token
            for interval in self.interval_tree[:value + DELTA]
            for token in self._get_tokens(interval[2])
        }

    def lt(self, value):
        return {
            token
            for interval in self.interval_tree[:value]
            for token in self._get_tokens(interval[2])
        }
