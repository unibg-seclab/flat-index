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


class Mapping(ABC):
    """Column mapping interface."""

    @abstractmethod
    def __init__(self, data):
        """Load mapping given its internal representation.

        :data: Internal representation of the column mapping.
        """
        pass

    @abstractmethod
    def _get_tokens(self, token):
        """Return tokens associated with a single generalization.
        
        :token: List of tokens to passthrough or a tuple of parameter used to
            generate tokens on the fly
        :return: List of tokens associated with a single generalization.
        """
        pass

    @abstractmethod
    def get_generalizations(self):
        """Return generalizations the mapping "stores" on the given column.
        
        :return: List of generalizations on the given column.
        """
        pass

    @abstractmethod
    def get_tokens(self):
        """Return tokens the mapping stores on the given column.
        
        :return: List of tokens the mapping stores on the given column.
        """
        pass

    @abstractmethod
    def between(self, extremes):
        """Return tokens generalizing values between extremes.
        
        :column: Column name of the mapping to use.
        :extremes: Tuple of two elements representing left and right
            inclusive extremes of the range.
        :return: Set of tokens generalizing values between extremes.
        """
        pass

    @abstractmethod
    def eq(self, value):
        """Return tokens generalizing the given value.
        
        :column: Column name of the mapping to use.
        :value: Value to retrieve within the mapping.
        :return: Set of tokens generalizing the given value.
        """
        pass

    @abstractmethod
    def neq(self, value):
        """Return tokens generalizing everything except the value.
        
        :column: Column name of the mapping to use.
        :value: Value to avoid retrieving in the mapping.
        :return: Set of tokens generalizing everything except the given value.
        """
        pass

    @abstractmethod
    def ge(self, value):
        """Return tokens generalizing values greater or equal to value.
        
        :column: Column name of the mapping to use.
        :value: Inclusive lower bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values greater or equal to value.
        """
        pass

    @abstractmethod
    def gt(self, value):
        """Return tokens generalizing values greater than value.
        
        :column: Column name of the mapping to use.
        :value: Lower bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values greater than value.
        """
        pass

    @abstractmethod
    def le(self, value):
        """Return tokens generalizing values lower or equal to value.
        
        :column: Column name of the mapping to use.
        :value: Inclusive upper bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values lower or equal to value.
        """
        pass

    @abstractmethod
    def lt(self, value):
        """Return tokens generalizing values lower than value.
        
        :column: Column name of the mapping to use.
        :value: Upper bound of the generalizations to retrieve.
        :return: Set of tokens generalizing values lower than value.
        """
        pass

    def in_values(self, values):
        """Return tokens generalizing the given values.
        
        :column: Column name of the mapping to use.
        :value: List of values to retrieve within the mapping.
        :return: Set of tokens generalizing the given values.
        """
        tokens = set()
        for value in values:
            tokens.update(self.eq(value))
        return tokens
