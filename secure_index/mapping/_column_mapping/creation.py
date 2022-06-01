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
from pickle import FALSE
import random
import math

import bitmap
import nacl.hash
import nacl.utils

import pyroaring
from intervaltree import Interval, IntervalTree

if __package__:
    from ._column_mapping.interval_tree import DELTA
else:
    from secure_index.mapping._column_mapping.interval_tree import DELTA


# Convert list of items to a multidimensional list
# It helps providing a consistent representation with mapping to group ids
def multi(items):
    return [[item] for item in items]


def tokenize(generalizations, keep_plain, to_hash, key, to_gid,
             generate_at_runtime, frequencies):
    if keep_plain:
        # Keep using the plain generalization strings
        return multi(generalizations)

    if to_gid is not None:
        return [to_gid[generalization] for generalization in generalizations]

    if to_hash:
        # Compute hashes of the generalization strings
        hashes = [None] * len(generalizations)
        for i, value in enumerate(generalizations):
            hash = nacl.hash.blake2b(value.encode("utf-8"),
                                    key=key,
                                    salt=nacl.utils.random(16),
                                    encoder=nacl.encoding.RawEncoder)
            hashes[i] = base64.b64encode(hash).decode("ascii")
            # NOTE: storing hashes with base64 encoding worsen space
            #       occupation of the map, but avoids computing this
            #       encoding multiple times down the chain
        return multi(hashes)

    if generate_at_runtime:
        # Generate random tokens ensuring no conflicts (on the starting
        # one)
        token_size = 32 # bits
        counter_size = math.ceil(math.log2(len(generalizations)))
        limit = 2**(token_size - counter_size) - 1
        tokens = [
            (random.randint(0, limit) << counter_size) + counter
            for counter in range(len(generalizations))
        ]
        # Randomly assign tokens to generalizations
        random.shuffle(tokens)
        # Store the number of groups using each generalization
        return [(tokens[i], frequencies[i]) for i in range(len(tokens))]

    # Randomly assign a token to each generalization
    tokens = list(range(len(generalizations)))
    random.shuffle(tokens)
    return multi(tokens)


# RANGE MAPPINGS CREATION

def is_range(string):
    return string.startswith('[') and (string.endswith(']')
                                       or string.endswith(')'))


# TODO: consider adding a boolean to determine if right is
#       included or not
def extract_extremes(value):
    if not is_range(value):
        return (value, value)

    # Filter pair of square brackets
    value = value[1:-1]
    # Find extremes of the range
    minus_indexes = [i for i, c in enumerate(value) if c == "-"]
    separator_idx = minus_indexes[1] if minus_indexes[0] == 0 else minus_indexes[0]
    extremes = [value[:separator_idx], value[separator_idx + 1:]]
    return extremes


def extract_ranges(generalizations):
    extremes = [extract_extremes(value) for value in generalizations]
    # Retrieve extremes type
    is_integer = True
    for current_extremes in extremes:
        start, end = map(float, current_extremes)
        if not start.is_integer() or not end.is_integer():
            is_integer = False
            break
    cast = int if is_integer else float
    return [(cast(start), cast(end)) for start, end in extremes]


def create_range_mapping(df,
                         column,
                         keep_plain=False,
                         to_hash=False,
                         key=None,
                         use_gid=False,
                         generate_at_runtime=False):
    frequencies = df[column].value_counts(sort=False)
    unique = frequencies.index

    try:
        ranges = extract_ranges(unique)

        to_gid = None
        if use_gid:
            # Create set mapping to group ids"
            to_gid = {}
            for name, group in df.groupby(column):
                to_gid[name] = set(map(int, group["GID"]))

        tokens = tokenize(unique, keep_plain, to_hash, key, to_gid,
                          generate_at_runtime, frequencies)

        # Sort range and tokens pairs by range starting point
        pairs = sorted(zip(ranges, tokens))
        # Transform list of pairs to range and tokens lists
        ranges, tokens = zip(*pairs)
        # Indexes ordering ranges by their ending point
        by_end = sorted(range(len(unique)),
                        key=lambda i: ranges[i][1],
                        reverse=True)

        # Embed dedicated column 16 bytes salt
        salt = nacl.utils.random(16)

        return tokens, ranges, by_end, generate_at_runtime, key, salt

    except ValueError:
        raise Exception(f"{column} does not contain numeric ranges.")


def create_interval_tree_mapping(df,
                                 column,
                                 keep_plain=False,
                                 to_hash=False,
                                 key=None,
                                 use_gid=False,
                                 generate_at_runtime=False):
    frequencies = df[column].value_counts(sort=False)
    unique = frequencies.index

    try:
        ranges = extract_ranges(unique)
        for i, range in enumerate(unique):
            # NOTE: Intervaltree supports ranges in the form [NUM, NUM).
            # We "adapt" it according to our [NUM, NUM] needs adding DELTA to
            # the right extreme. This must be taken into account when executing
            # queries.
            start, end = range
            ranges[i] = (start, end + DELTA)

        to_gid = None
        if use_gid:
            # Create set mapping to group ids"
            to_gid = {}
            for name, group in df.groupby(column):
                to_gid[name] = set(map(int, group["GID"]))

        tokens = tokenize(unique, keep_plain, to_hash, key, to_gid,
                          generate_at_runtime, frequencies)

        interval_tree = IntervalTree()
        for i, _ in enumerate(ranges):
            interval_tree.add(Interval(ranges[i][0], ranges[i][1], tokens[i]))

        # Embed dedicated column 16 bytes salt
        salt = nacl.utils.random(16)

        return interval_tree, generate_at_runtime, key, salt

    except ValueError:
        raise Exception(f"{column} does not contain numeric ranges.")


# SET MAPPINGS CREATION

def is_set(string):
    return string.startswith('{') and string.endswith('}')


def get_items(string):
    return set(string[1:-1].split(',')) if is_set(string) else {string}


def create_categorical_mapping(df,
                               column,
                               create_indexes,
                               keep_plain=False,
                               to_hash=False,
                               key=None,
                               use_gid=False,
                               generate_at_runtime=False):
    # Identify columns anonymized as sets
    if df[column].dtype != "object":
        raise Exception(f"{column} does not contain sets.")

    frequencies = df[column].value_counts(sort=False)
    unique = frequencies.index

    # Retrieve set of categories
    categories = set()
    for i, generalization in enumerate(unique):
        items = get_items(generalization)
        categories.update(items)

    # From category to its set index
    categories = {
        value: i
        for i, value in enumerate(sorted(list(categories)))
    }

    indexes = create_indexes(categories, unique)

    to_gid = None
    if use_gid:
        # Create set mapping to group ids"
        to_gid = {}
        for name, group in df.groupby(column):
            to_gid[name] = set(map(int, group["GID"]))

    tokens = tokenize(unique, keep_plain, to_hash, key, to_gid,
                      generate_at_runtime, frequencies)

    # Embed dedicated column 16 bytes salt
    salt = nacl.utils.random(16)

    return tokens, categories, indexes, generate_at_runtime, key, salt


def create_sets(categories, generalizations):
    indexes = [set() for _ in categories]
    for i, value in enumerate(generalizations):
        items = get_items(value)
        for item in items:
            indexes[categories[item]].add(i)
    return indexes


def create_bitmaps(categories, generalizations):
    bitmaps = [bitmap.BitMap(len(generalizations)) for _ in categories]
    for i, value in enumerate(generalizations):
        items = get_items(value)
        for item in items:
            bitmaps[categories[item]].set(i)
    return bitmaps


def create_roaring_bitmaps(categories, generalizations):
    indexes = [pyroaring.BitMap() for _ in categories]
    for i, value in enumerate(generalizations):
        items = get_items(value)
        for item in items:
            indexes[categories[item]].add(i)
    return indexes
