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

import base64
import pickle
import sys

import nacl.pwhash
import nacl.secret

if __package__:
    from .interface import MultidimensionalMapping
    from ._column_mapping.bitmap import BitmapMapping
    from ._column_mapping.interval_tree import IntervalTreeMapping
    from ._column_mapping.range import RangeMapping
    from ._column_mapping.set import SetMapping
else:
    from secure_index.mapping.interface import MultidimensionalMapping
    from secure_index.mapping._column_mapping.bitmap import BitmapMapping
    from secure_index.mapping._column_mapping.interval_tree import IntervalTreeMapping
    from secure_index.mapping._column_mapping.range import RangeMapping
    from secure_index.mapping._column_mapping.set import SetMapping


MAPPINGS = {
    "bitmap": BitmapMapping,
    "interval-tree": IntervalTreeMapping,
    "range": RangeMapping,
    "roaring": SetMapping,
    "set": SetMapping,
}


class HeterogeneousMapping(MultidimensionalMapping):
    """Heterogeneous mapping built as the composition of column mappings.
    
    :schema: List of strings representing column names of the original dataset.
    :mappings: Internal data structure to store column mappings.
    :types: Dictionary stating the mapping type of each column.

    Available mapping types are: bitmap, interval-tree, range, roaring and set.
    """

    def __init__(self, path, key=None):
        if key is None:
            # Read plaintext mapping
            with open(path, 'rb') as file:
                self.schema, mapping = pickle.load(file)
            self.mappings, self.types, self.is_gids = mapping
            return

        # Read encrypted mapping
        with open(path, 'r') as f:
            content = f.read()
            encrypted = base64.b64decode(content)

        # Decrypt mapping
        box = nacl.secret.SecretBox(key)
        try:
            plaintext = box.decrypt(encrypted)
        except nacl.exceptions.CryptoError:
            print("ERROR: Wrong password.")
            sys.exit()

        # Reconstitute object from its pickled representation
        self.schema, mapping = pickle.loads(plaintext)
        self.mappings, self.types, self.is_gids = mapping

    def _get_column_mapping(self, column):
        try:
            mapping_type = self.types[column]
            mapping_data = self.mappings[column]
        except KeyError:
            raise Exception(f"{column} does not exist in the mapping.")
        
        try:
            return MAPPINGS[mapping_type](mapping_data)
        except KeyError:
            raise Exception(f"{mapping_type} is not a valid mapping type.")

    def get_generalizations(self, column):
        mapping = self._get_column_mapping(column)
        return mapping.get_generalizations()

    def get_runtime_tokens_dictionary(self, column):
        mapping = self._get_column_mapping(column)        
        return mapping.get_runtime_tokens_dictionary()

    def get_tokens(self, column):
        mapping = self._get_column_mapping(column)
        return mapping.get_tokens()

    def is_gid(self, column):
        try:
            return self.is_gids[column]
        except KeyError:
            raise Exception(f"{column} does not exist in the mapping.")

    def between(self, column, extremes):
        mapping = self._get_column_mapping(column)
        return mapping.between(extremes)

    def eq(self, column, value):
        mapping = self._get_column_mapping(column)
        return mapping.eq(value)

    def neq(self, column, value):
        mapping = self._get_column_mapping(column)
        return mapping.neq(value)

    def ge(self, column, value):
        mapping = self._get_column_mapping(column)
        return mapping.ge(value)

    def gt(self, column, value):
        mapping = self._get_column_mapping(column)
        return mapping.gt(value)

    def le(self, column, value):
        mapping = self._get_column_mapping(column)
        return mapping.le(value)

    def lt(self, column, value):
        mapping = self._get_column_mapping(column)
        return mapping.lt(value)

    def in_values(self, column, values):
        mapping = self._get_column_mapping(column)
        return mapping.in_values(values)
