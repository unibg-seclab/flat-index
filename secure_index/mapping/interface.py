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

from abc import ABC
from abc import abstractmethod


class MultidimensionalMapping(ABC):
    """Multidimensional mapping interface."""

    @abstractmethod
    def __init__(self, path, key=None):
        """Load mapping at the given path.

        :path: Path to the mapping file.
        :key: Optional key to decrypt the encrypted mapping. It determines
            wether the mapping is treated as plaintext or encrypted. Defaults
            to None.
        """
        pass

    @abstractmethod
    def get_generalizations(self, column):
        """Return generalizations the mapping "stores" on the given column.
        
        :return:  A Multi-List of generalizations on the given column.
        """
        pass

    @abstractmethod
    def get_runtime_tokens_dictionary(self, column):
        """Return a dictionary mapping the generalization to all the runtime tokens.
        
        :return: Dictionary (generalization value) -> iterator(list of tokens) the mapping stores on the given column.

        """
        pass


    @abstractmethod
    def get_tokens(self, column):
        """Return tokens the mapping stores on the given column.
        
        :return: List of tokens the mapping stores on the given column.
        """
        pass

    @abstractmethod
    def is_gid(self, column):
        """Return whether the mapping on the given column is a mapping to gid.
        
        :return: False when the mapping on the given column is not a mapping to
            gid, True otherwise.
        """
        pass

    @abstractmethod
    def between(self, column, extremes):
        """Return tokens generalizing values between extremes.
        
        :column: Column name of the mapping to use.
        :extremes: Tuple of two elements representing left and right
            inclusive extremes of the range.
        :return: Set of tokens generalizing values between extremes.
        """
        pass

    @abstractmethod
    def eq(self, column, value):
        """Return tokens generalizing the given value.
        
        :column: Column name of the mapping to use.
        :value: Value to retrieve within the mapping.
        :return: Set of tokens generalizing the given value.
        """
        pass

    @abstractmethod
    def neq(self, column, value):
        """Return tokens generalizing everything except the value.
        
        :column: Column name of the mapping to use.
        :value: Value to avoid retrieving in the mapping.
        :return: Set of tokens generalizing everything except the given value.
        """
        pass

    @abstractmethod
    def ge(self, column, value):
        """Return tokens generalizing values greater or equal to value.
        
        :column: Column name of the mapping to use.
        :value: Inclusive lower bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values greater or equal to value.
        """
        pass

    @abstractmethod
    def gt(self, column, value):
        """Return tokens generalizing values greater than value.
        
        :column: Column name of the mapping to use.
        :value: Lower bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values greater than value.
        """
        pass

    @abstractmethod
    def le(self, column, value):
        """Return tokens generalizing values lower or equal to value.
        
        :column: Column name of the mapping to use.
        :value: Inclusive upper bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values lower or equal to value.
        """
        pass

    @abstractmethod
    def lt(self, column, value):
        """Return tokens generalizing values lower than value.
        
        :column: Column name of the mapping to use.
        :value: Upper bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values lower than value.
        """
        pass

    @abstractmethod
    def in_values(self, column, values):
        """Return tokens generalizing the given values.
        
        :column: Column name of the mapping to use.
        :value: List of values to retrieve within the mapping.
        :return: Set of tokens generalizing the given values.
        """
        pass
