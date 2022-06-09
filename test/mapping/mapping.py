#!/usr/bin/env python3
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


import _pickle as cPickle
import argparse
import bz2
import os
import os.path
import pickle
import secrets
import sys
from functools import partial
from timeit import default_timer as timer

import bitmap
import pandas as pd
import pyroaring
from intervaltree import Interval
from intervaltree import IntervalTree


# RANGE MAPPINGS

def is_range(string):
    return string.startswith('[') and (string.endswith(']')
                                   or string.endswith(')'))


def get_number(string):
    num = float(string)
    if num.is_integer():
        num = int(num)
    return num

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


def create_interval_tree_mapping(df, column, use_gid=False):
    counts = df[column].value_counts(sort=False)
    unique = counts.index

    try:
        ranges = [None] * len(unique)
        for i, value in enumerate(unique):
            # TODO: consider adding a boolean to determine if right is included or not
            if not is_range(value):
                num = get_number(value)
                ranges[i] = (num, num + 1)
            else:
                start, end = map(get_number, extract_extremes(value))
                ranges[i] = (start, end + 1)
        if use_gid:
                to_gid = {}
                for name, group in df.groupby(column):
                        if not is_range(name):
                                num = get_number(name)
                                act_name = (num, num + 1)
                        else:
                                start, end = map(get_number, extract_extremes(name))
                                act_name = (start, end + 1)
                        to_gid[act_name] = set(map(int, group["GID"]))
        interval_tree = IntervalTree()
        for i, range_value in enumerate(ranges):
            if use_gid:
                interval_tree.add(Interval(ranges[i][0], ranges[i][1],
                                       to_gid[(ranges[i][0], ranges[i][1])]))
            else:
                interval_tree.add(Interval(ranges[i][0], ranges[i][1], 
                                       (counts[i], secrets.randbits(32))))

        return interval_tree

    except ValueError:
        print(f"{column} is not numeric. Skipping...")


def create_range_mapping(df, column, use_gid=False):
    counts = df[column].value_counts(sort=False)
    unique = counts.index

    try:
        ranges = [None] * len(unique)
        for i, value in enumerate(unique):
            # TODO: consider adding a boolean to determine if right is included or not
            if not is_range(value):
                num = get_number(value)
                ranges[i] = (num, num)
            else:
                m_count = value[1:-1].count("-")
                if m_count == 1:
                    ranges[i] = tuple(map(get_number, value[1:-1].split('-')))
                else:
                    m_indexes = [i for i in range(0,len(value)) if value[i]=="-"]
                    if m_indexes[0] != 1:
                        to_map = [value[1:m_indexes[0]], value[m_indexes[0] + 1:-1]]
                    else:
                        to_map = [value[m_indexes[0]:m_indexes[1]],value[m_indexes[1] + 1:-1]]
                    ranges[i] = tuple(map(get_number, to_map))
        ranges = sorted(ranges)
        by_end = sorted(range(len(unique)), key=lambda i: ranges[i][1], reverse=True)
        if use_gid:
                to_gid = {}
                for name, group in df.groupby(column):
                        to_gid[name] = set(map(int, group["GID"]))
                return ranges, by_end, to_gid

        return ranges, by_end, list(counts), [secrets.randbits(32) for i in range(0, len(list(counts)))]

    except ValueError:
        print(f"{column} is not numeric. Skipping...")


# SET MAPPINGS

def is_set(string):
    return string.startswith('{') and string.endswith('}')


def get_items(string):
    return set(string[1:-1].split(',')) if is_set(string) else {string}


def create_categorical_mapping(df, column, create_indexes, use_gid=False):
    counts = df[column].value_counts(sort=False)
    unique = counts.index

    # Retrieve set of categories
    categories = set()
    for i, generalization in enumerate(unique):
        items = get_items(generalization)
        categories.update(items)

    # From category to its bitmap index
    categories = {value:i for i, value in enumerate(sorted(list(categories)))}
    if use_gid:
        to_gid = {}
        for name, group in df.groupby(column):
                to_gid[name] = set(map(int, group["GID"]))
        return categories, create_indexes(categories, unique), to_gid

    return categories, create_indexes(categories, unique), list(counts), [secrets.randbits(32) for i in range(0, len(list(counts)))]


def create_bitmaps(categories, generalizations):
    indexes = [bitmap.BitMap(len(generalizations)) for _ in categories]
    for i, value in enumerate(generalizations):
        items = get_items(value)
        for item in items:
            indexes[categories[item]].set(i)
    return indexes


def create_roaring_bitmaps(categories, generalizations):
    indexes = [pyroaring.BitMap() for _ in categories]
    for i, value in enumerate(generalizations):
        items = get_items(value)
        for item in items:
            indexes[categories[item]].add(i)
    return indexes


def create_sets(categories, generalizations):
    indexes = [set() for _ in categories]
    for i, value in enumerate(generalizations):
        items = get_items(value)
        for item in items:
            indexes[categories[item]].add(i)
    return indexes


def prepare_bench(df_name):
    test = os.path.realpath(os.path.join(__file__, *([os.path.pardir] * 2)))
    results = os.path.join(test, "results", "mapping", "times")
    if not os.path.isdir(results):
        os.mkdir(results)
    if not os.path.isfile(os.path.join(results, f"{df_name}_mapping_times.csv")):
        data = {"col":[], "type": [], "time": [], "portion": []}
    else:
        data = pd.read_csv(os.path.join(results, f"{df_name}_mapping_times.csv")).to_dict(orient="list")
    return data, results


def do_bench(benchmark, column, mapping_type, portion, time):
    for col, m_type, p, t in zip(benchmark["col"], benchmark["type"], benchmark["portion"], benchmark["time"]):
        if col == column and m_type == mapping_type and p == portion:
            t = time
            break
    else:
        benchmark["col"].append(column)
        benchmark["type"].append(mapping_type)
        benchmark["time"].append(time)
        benchmark["portion"].append(portion)
    return benchmark


CREATE = {
    "range": create_range_mapping,
    "interval-tree": create_interval_tree_mapping,
    "bitmap": partial(create_categorical_mapping, create_indexes=create_bitmaps),
    "roaring": partial(create_categorical_mapping, create_indexes=create_roaring_bitmaps),
    "set": partial(create_categorical_mapping, create_indexes=create_sets),
}


parser = argparse.ArgumentParser(
    description='Create mapping on k-anonymous dataset.'
)
parser.add_argument('input',
                    metavar='INPUT',
                    help='path to the k-anonimous dataset')
parser.add_argument('output',
                    metavar='OUTPUT',
                    help='path where to store the mapping')
parser.add_argument('-c', '--column', metavar='COLUMN', help='column name')
parser.add_argument('-t',
                    '--type',
                    metavar='TYPE',
                    choices=['bitmap', 'interval-tree', 'range', 'roaring',
                             'set'],
                    default='range',
                    help='type of mapping: bitmap, interval-tree, '
                         'range (default), roaring and set')
parser.add_argument('-b', '--bz2format',
                    dest='to_compress',
                    action='store_true',
                    help='save the pickle compressed using bz2')
parser.add_argument('-p', '--portion',
                    dest='portion',
                    type=int,
                    default=0,
                    help='portion of dataset used')
parser.add_argument('-g', '--gid',
                    dest='use_gid',
                    action='store_true',
                    help='use GID represantation')


args = parser.parse_args()

dataset = args.input
destination = args.output
column = args.column
to_compress = args.to_compress
mapping_type = args.type
portion = args.portion
use_gid = args.use_gid

if dataset.split(".")[-1] != "csv":
    print("Not a csv file. Skipping...")
    sys.exit()

print("[*] Read anonymized dataset")
df = pd.read_csv(dataset, dtype=object)
portion = len(df.index) if not portion else portion
if "GID" not in df:
    print("No GID column. Skipping...")
    sys.exit()

if column and column not in df:
    print(f"No {column} column. Skipping...")
    sys.exit()

df.drop_duplicates("GID", inplace=True)

columns = [column for column in df.columns if column not in ["INDEX", "GID"]]
columns = [column] if column else columns

mapping = {}

df_name = os.path.basename(dataset).split(".")[0]

for column in columns:
    print(f"[*] Map {column} using {mapping_type}")

    start_map_creation = timer()
    mapping[column] = CREATE[mapping_type](df, column, use_gid=use_gid)
    end_map_creation = timer()
    benchmark, results = prepare_bench(df_name)
    benchmark = do_bench(benchmark, column, mapping_type, portion, end_map_creation - start_map_creation)
    pd.DataFrame(data = benchmark).to_csv(os.path.join(results, f"{df_name}_mapping_times.csv"), index=False)

# write mapping to file
if not(to_compress):
    with open(destination, 'wb') as output:
        pickle.dump(mapping, output)
else:
    with bz2.BZ2File(destination, 'w') as output:
        cPickle.dump(mapping, output)
