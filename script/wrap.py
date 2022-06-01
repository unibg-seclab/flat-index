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


import argparse
import base64
import functools
import getpass
import json
import multiprocessing
import os.path
import pickle
import random
import time
from collections import Counter
from functools import partial

import lz4.frame
import pandas as pd
import msgpack
import nacl.pwhash
import nacl.secret
import nacl.utils
import snappy
import zstd

from secure_index.mapping.heterogeneous import HeterogeneousMapping


MAPPINGS = {
    "heterogeneous": HeterogeneousMapping,
}

SERIALIZE = {
    "json": lambda tuples: json.dumps(tuples).encode("utf-8"),
    "pickle": pickle.dumps,
    "msgpack": msgpack.dumps
}

COMPRESS = {
    "none": lambda bytes: bytes,
    "lz4": partial(lz4.frame.compress,
                   compression_level=lz4.frame.COMPRESSIONLEVEL_MINHC,
                   store_size=False),
    "snappy": snappy.compress,
    "zstd": lambda bytes: zstd.compress(bytes, 3, 1)
}


def init_lock(l):
    global locks
    locks = l


def check_idx_correctness(mapping, generalizations_idx, tokens_idx):
    for column in indices:
        mapping_column = mapping[column]
        generalizations_idx_column = generalizations_idx[column]
        tokens_idx_column = tokens_idx[column]

        for generalization, tokens in mapping_column.items():
            generalization_idx = generalizations_idx_column[generalization]
            idx = tokens_idx_column[generalization_idx]
            assert idx % len(tokens) == 0


def get_blob_size(param):
    gid, group = param
    plain = [column for column in group.columns if column.endswith("_plain")]
    tuples = [tuple(row) for index, row in group[plain].iterrows()]
    return len(compress(serialize(tuples)))


def get_current_item(mapping, column, generalization):
    items = mapping[column][generalization]
    generalization_idx = generalizations_idx[column][generalization]
    if 'locks' in globals(): locks[column].acquire()
    token_idx = next_tokens_idx[column][generalization_idx]
    next_tokens_idx[column][generalization_idx] += 1
    if 'locks' in globals(): locks[column].release()
    return items[token_idx % len(items)]


# when the GID is kept, assumes that there is a column named GID
def wrap_dataset(param, blob_size):
    gid, group = param
    # Plain and anon column names
    plain = [column for column in group.columns if column.endswith("_plain")]
    anon = [column + "_anon" for column in indices]

    # Retrieve group generalization
    generalizations = group.iloc[0][anon]
    GroupID = group.iloc[0]["GID"] if keep_GID else None
    # Use indices according to the requested wrapping representation
    row_indices = []
    if to_gid or compact:
        row_indices.append(gid)
    if not compact:
        # Retrive tokens of the generalizations
        for column, generalization in zip(indices, generalizations):
            token = get_current_item(t_mapping, column, generalization)
            row_indices.append(token)
    if kvstore:
        row_indices = [gid]

    # Bundle tuples of each group into a list
    tuples = [tuple(row) for _, row in group[plain].iterrows()]

    compressed = compress(serialize(tuples))

    # Ensure every blob has the same length by padding it
    lpadding = blob_size - len(compressed) if blob_size else 0
    padding = nacl.utils.random(lpadding) if blob_size else b''
    try:
        lpadding = lpadding.to_bytes(2, byteorder='little', signed=False)
    except OverflowError:
        Exception("Padding size does not fit into 2 bytes.")
    blob = lpadding + compressed + padding

    # Encrypt
    nonce = nacl.utils.random(nacl.secret.SecretBox.NONCE_SIZE)
    enc_tuple = box.encrypt(blob, nonce)

    return (*row_indices, base64.b64encode(enc_tuple).decode("ascii")) if not keep_GID else (GroupID, *row_indices, base64.b64encode(enc_tuple).decode("ascii"))


parser = argparse.ArgumentParser(
    description='Wrap dataset according to the wrapping mapping.'
)
parser.add_argument('dataset',
                    metavar='PLAIN',
                    help='path to the plain dataset')
parser.add_argument('anonymized',
                    metavar='ANONIMIZED',
                    help='path to the anonymized dataset')
parser.add_argument('mapping',
                    metavar='MAPPING',
                    help='path to the encrypted mapping')
parser.add_argument('output',
                    metavar='OUPUT',
                    help='where to store the dataset to upload')
parser.add_argument('-c',
                    '--compression',
                    metavar='ALGORITHM',
                    choices=['none', 'lz4', 'snappy', 'zstd'],
                    default='zstd',
                    help='compression algorithm: none, lz4, snappy, zstd '
                         '(default)')
parser.add_argument('-g',
                    '--GID-keep',
                    action='store_true',
                    dest="keep_GID",
                    help='flag that indicates if the GID columns is kept')
parser.add_argument('-j',
                    '--jobs',
                    metavar='JOBS',
                    help='number of parallel jobs to wrap the given dataset')
parser.add_argument('-k',
                    '--kvstore',
                    dest='kvstore',
                    action='store_true',
                    help='prepare the files with the kv-store as the target')
parser.add_argument('-m',
                    '--mapping-table',
                    dest='table',
                    action='store_true',
                    help='produce a mapping table')
parser.add_argument('-n',
                    '--normal-form',
                    dest='normal',
                    action='store_true',
                    help='represent dataset in normal form')
parser.add_argument('-p',
                    '--pad',
                    action='store_true',
                    help='pad enctuples to achive absolute flattening')
parser.add_argument('--password',
                    help='password necessary to read the mapping')
parser.add_argument('-s',
                    '--serialization',
                    metavar='FORMAT',
                    choices=['json', 'msgpack', 'pickle'],
                    default='json',
                    help='serialization format: json (default), msgpack, '
                         'pickle')
parser.add_argument('-t',
                    '--type',
                    metavar='TYPE',
                    default='heterogeneous',
                    help='type of mapping: heterogeneous (default)')

args = parser.parse_args()

dataset = args.dataset
anonymized = args.anonymized
path = args.mapping
mapping_type = args.type
output = args.output
jobs = min(args.jobs if args.jobs else float("+inf"),
           multiprocessing.cpu_count())
mapping_table = args.table
kvstore = args.kvstore
normal = args.normal
serialize = SERIALIZE[args.serialization]
compress = COMPRESS[args.compression]
pad = args.pad
keep_GID = args.keep_GID
pw = args.password.encode("utf-8") if args.password else None

compact = mapping_table + normal
if compact > 1:
    parser.error("either table mapping or normal form should be set, not " +
                 "both.")

if mapping_type not in MAPPINGS:
    parser.error(f"{mapping_type} is not a valid mapping type.")

print("[*] Read plain dataset")
start = time.time()
df = pd.read_csv(dataset, index_col="INDEX")
print("Read plain dataset:\t {:10.3f}s".format(time.time() - start))

# Reduce in-memory footprint of the plain dataset
for column in df.columns:
    if df[column].dtype == "int64":
        if df[column].min() >= 0:
            df[column] = pd.to_numeric(df[column], downcast="unsigned")
        else:
            df[column] = pd.to_numeric(df[column], downcast="signed")
    elif df[column].dtype == "float64":
        df[column] = pd.to_numeric(df[column], downcast="float")

print("[*] Read anonymized dataset")
start = time.time()
dtype = {column:str for column in df.columns}
adf = pd.read_csv(anonymized, index_col="INDEX", dtype=dtype)
print("Read anonymized dataset: {:10.3f}s".format(time.time() - start))

# Reduce in-memory footprint of the anonymized dataset
# NOTE: Do not use categorical as this has a huge performance hit
if "GID" in adf.columns:
    adf["GID"] = pd.to_numeric(adf["GID"], downcast="unsigned")

if not pw:
    try:
        pw = getpass.getpass("Password: ").encode("utf-8")
    except UnicodeError:
        raise RuntimeError("Only utf-8 compatible passwords allowed")
salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'
# Generate the key
kdf = nacl.pwhash.argon2id.kdf
key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)
box = nacl.secret.SecretBox(key)

mapping = MAPPINGS[mapping_type](path, key)

# Retrieve all those column not using a mapping to gid and promote them to
# column indices
indices = [column for column in mapping.mappings if not mapping.is_gid(column)]

t_mapping = {}
generalizations_idx = {}
next_tokens_idx = {}

for column in indices:
    print(f"[*] Map {column} generalizations to their tokens")
    start = time.time()
    generalizations = mapping.get_generalizations(column)
    print("Retrieve generalizations:{:10.3f}s".format(time.time() - start))
    start = time.time()
    tokens = mapping.get_tokens(column)
    print("Retrieve tokens: \t {:10.3f}s".format(time.time() - start))
    start = time.time()
    flat_tokens = [token
                   for generalization_tokens in tokens
                   for token in generalization_tokens]
    counts = Counter(flat_tokens)
    for token, count in counts.items():
        if count > 1:
            print(f"Token {token} repeats {count} times")
    print("Collisions detection: \t {:10.3f}s".format(time.time() - start))
    start = time.time()
    t_mapping[column] = {
        avalue: tokens[i]
        for i, avalue in enumerate(generalizations)
    }
    print("Create t_mapping: \t {:10.3f}s".format(time.time() - start))
    start = time.time()
    generalizations_idx[column] = {
        avalue: i
        for i, avalue in enumerate(generalizations)
    }
    next_tokens_idx[column] = multiprocessing.Array("I",
                                                    len(generalizations),
                                                    lock=False)
    print("Create t_mapping_idx: \t {:10.3f}s".format(time.time() - start))

start = time.time()
if mapping_table and not kvstore:
    # assume no columns go by the name of GroupId
    print("[*] Create group id and anonymization tokens table")
    tuples = []
    for index, group in adf.groupby("GID"):
        row = group.iloc[0]
        tokens = []
        for column in indices:
            generalization = row[column]
            token = get_current_item(t_mapping, column, generalization)
            tokens.append(token)
        tuples.append((index, *tokens))

    columns = ['GroupId'] + list(indices)
    table = pd.DataFrame(tuples, columns=columns)
    print(f"[*] Write mapping table")
    parent = os.path.dirname(output)
    table.to_csv(os.path.join(parent, "mapping.csv"), index=False)

    print("[*] Checking correctness of the indexes (mapping)")
    check_idx_correctness(t_mapping, generalizations_idx, next_tokens_idx)

if mapping and kvstore:
    print("[*] Create anonymization tokens and group id mapping")
    parent = os.path.dirname(output)
    for column in indices:
        tuples = []
        for generalization, group in adf.groupby(column):
            tokens = t_mapping[column][generalization]
            gids = set(group["GID"])
            if len(tokens) == 1:
                tuples.append((tokens[0], gids))
            else:
                assert len(tokens) == len(gids)
                tuples.extend((token, {gid})
                              for token, gid in zip(tokens, gids))

        column_hash = pd.DataFrame(tuples, columns=["Key", "Value"])
        print(f"[*] Write {column} mapping")
        column_hash.to_csv(os.path.join(parent, column + ".csv"), index=False)

if normal:
    # assume no columns go by the name of Id
    # assume no columns go by the name of Group
    print(f"[*] Create group id and anonymization tokens in normal form")
    parent = os.path.dirname(output)
    id = {}
    for column in indices:
        generalizations = adf[column].unique()

        number_of_tokens = 0
        for generalization, tokens in t_mapping[column].items():
            number_of_tokens += len(tokens)

        ids = list(range(number_of_tokens))
        random.shuffle(ids)
        i = 0
        id[column] = {}
        for generalization in generalizations:
            tokens = t_mapping[column][generalization]
            id[column][generalization] = ids[i:i + len(tokens)]
            i += len(tokens)

        # Table containing unique generalization tokens
        tuples = [None] * number_of_tokens
        for generalization in generalizations:
            for index, token in zip(id[column][generalization],
                                    t_mapping[column][generalization]):
                tuples[index] = (index, token)
        table = pd.DataFrame(tuples, columns=['Id', column])
        print(f"[*] Write {column} table")
        table.to_csv(os.path.join(parent, column + ".csv"), index=False)

    print("[*] Checking correctness of the indexes (single column tables)")
    check_idx_correctness(id, generalizations_idx, next_tokens_idx)

    # Table containing GroupId to generalization tokens mapping
    tuples = []
    for index, group in adf.groupby("GID"):
        row = group.iloc[0]
        ids = []
        for column in indices:
            generalization = row[column]
            ids.append(get_current_item(id, column, generalization))
        tuples.append((index, *ids))
    columns = ['GroupId'] + [column + "Id" for column in indices]
    table = pd.DataFrame(tuples, columns=columns)
    print(f"[*] Write GroupIdToColumns mapping table")
    table.to_csv(os.path.join(parent, "GroupIdToColumns.csv"), index=False)

    print("[*] Checking correctness of the indexes (intermediate mapping table)")
    check_idx_correctness(id, generalizations_idx, next_tokens_idx)

print("Auxiliary stuff:\t {:10.3f}s".format(time.time() - start))

# Whether we have some column mapping to gid or not
to_gid = (len(mapping.schema) != len(indices))

jdf = df.join(adf, lsuffix='_plain', rsuffix='_anon')

max_size = None
if pad:
    start = time.time()
    with multiprocessing.Pool(jobs) as pool:
        print(f"[*] Compute maximum size of the serialization")
        sizes = pool.map(get_blob_size, jdf.groupby("GID"))
        max_size = max(sizes)
    print("Maximum size: \t\t {:10.3f}s".format(time.time() - start))
    print(f"Maximum blob size: \t\t {max_size}")

print(f"[*] Wrap dataset")
start = time.time()
locks = {column: multiprocessing.Lock() for column in indices}
with multiprocessing.Pool(jobs, initializer=init_lock, initargs=(locks,)) as pool:
    enc = pool.map(functools.partial(wrap_dataset, blob_size=max_size),
                   jdf.groupby("GID"))
print("Wrapping:\t\t {:10.3f}s".format(time.time() - start))

print("[*] Checking correctness of the indexes (wrapping)")
check_idx_correctness(t_mapping, generalizations_idx, next_tokens_idx)

if not kvstore:
    columns = []
    if to_gid or compact or keep_GID:
        columns.append("GroupId")
    if not compact:
        columns.extend(indices)
    columns.append("EncTuples")
else:
    columns = ["Key", "Value"]

df = pd.DataFrame(enc, columns=columns)

print("[*] Write dataset")
start = time.time()
df.to_csv(output, index=False)
print("Writing:\t\t {:10.3f}s".format(time.time() - start))
