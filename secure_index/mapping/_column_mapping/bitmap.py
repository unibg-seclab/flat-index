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
import multiprocessing

if __package__:
    from .interface import Mapping
    from .runtime_token_to_representation import get_token_representations
    from .runtime_token_to_representation import get_all_tokens_representations
else:
    from secure_index.mapping._column_mapping.interface import Mapping
    from secure_index.mapping._column_mapping.runtime_token_to_representation import get_token_representations
    from secure_index.mapping._column_mapping.runtime_token_to_representation import get_all_tokens_representations


def _create_generalization(idx, categories, indexes):
    """Create generalization starting from its internal mapping representation.

    When multiple categories are in the generalization, the categories are
    sorted in lexicographic order.

    :categories: Dictionary mapping the index of the category to its value.
    :indexes: List of bitmaps storing indexes of the sets where categories are.
    :idx: Number of the generation to create.

    :return: String representing the generalization.
    """
    items = []
    for category_id, index in enumerate(indexes):
        if index.test(idx):
            items.append(categories[category_id])
    return "{" + ",".join(items) + "}" if len(items) > 1 else items[0]


class BitmapMapping(Mapping):
    """Bitmap mapping.
    
    :tokens: List of tokens associated with each set.
    :categories: Dictionary mapping categories to the index of their bitmap.
    :indexes: List of bitmaps storing indexes of the sets where categories are.
    :is_runtime: Indicate whether tokens should be generated at runtime.
    :key: Key to generate the tokens.
    :salt: Random salt to generate the tokens.
    """

    def __init__(self, data):
        self.tokens, self.categories, self.indexes, self.is_runtime, self.key, self.salt = data

    def _get_tokens(self, token):
        if self.is_runtime:
            return get_token_representations(token, self.key, self.salt)
        return token

    def get_generalizations(self):
        categories = {i: category for category, i in self.categories.items()}

        create_generalization = functools.partial(_create_generalization,
                                                   categories=categories,
                                                   indexes=self.indexes)

        with multiprocessing.Pool() as pool:
            generalizations = pool.map(create_generalization,
                                       range(len(self.tokens)))
        return generalizations

    def get_tokens(self):
        if self.is_runtime:
            return get_all_tokens_representations(self.tokens, self.key, self.salt)
        return self.tokens

    def between(self, extremes):
        """Raise exception method not implemented."""
        raise Exception(
            "Categorical mapping does not implement the between method."
        )

    def eq(self, value):
        value = str(value)
        if value not in self.categories:
            return set()
        return {
            token
            for i in self.indexes[self.categories[value]].nonzero()
            for token in self._get_tokens(self.tokens[i])
        }

    def neq(self, value):
        value = str(value)
        if value not in self.categories:
            return {token for tokens in self.get_tokens() for token in tokens}
        # Look for an index corresponding to the set with value as only
        # category
        target = self.categories[value]
        others = [i for i in range(len(self.categories)) if i != target]
        to_exclude = None
        for index in self.indexes[target].nonzero():
            for other in others:
                if self.indexes[other].test(index):
                    # Current index corresponds to a set with multiple
                    # categories
                    break
            else:
                # Found index corresponding to the set with value as only
                # category
                to_exclude = index
                break

        # Return everything but the token corresponding to the set with value
        # as only category (when those set exists)
        return {
            token
            for i in range(len(self.tokens)) if i != to_exclude
            for token in self._get_tokens(self.tokens[i])
        }

    def ge(self, value):
        """Raise exception method not implemented."""
        raise Exception(
            "Categorical mapping does not implement the ge method."
        )

    def gt(self, value):
        """Raise exception method not implemented."""
        raise Exception(
            "Categorical mapping does not implement the gt method."
        )

    def le(self, value):
        """Raise exception method not implemented."""
        raise Exception(
            "Categorical mapping does not implement the le method."
        )

    def lt(self, value):
        """Raise exception method not implemented."""
        raise Exception(
            "Categorical mapping does not implement the lt method."
        )
