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
import functools
import getpass
import json
import os
import pickle
import re
import sqlite3
import sys
from timeit import default_timer as timer

import lz4.frame
import msgpack
import nacl.pwhash
import nacl.secret
import pandas as pd
import redis
import snappy
import sqlalchemy
import zstd
from pympler import asizeof

from secure_index.mapping.heterogeneous import HeterogeneousMapping
from secure_index.rewriting import rewrite as official_rewrite


CHUNK_SIZE= 10000

MAPPINGS = {
    "heterogeneous": HeterogeneousMapping,
}

DESERIALIZE = {
    "json": lambda bytes: json.loads(bytes.decode("utf-8")),
    "pickle": pickle.loads,
    "msgpack": msgpack.loads
}

DECOMPRESS = {
    "none": lambda bytes: bytes,
    "lz4": lz4.frame.decompress,
    "snappy": snappy.decompress,
    "zstd": zstd.decompress
}

PLAIN_COLUMNS = [
    "index", "query", "size", "bytes_size", "nof_result_tuples", "plain"
]

WRAPPED_COLUMNS = [
    "index", "query", "size", "bytes_size", "nof_enctuples",
    "nof_plaintext_tuples", "nof_result_tuples", "rewriting", "server",
    "decryption", "creation", "filtering"
]


def evaluate(queries, selectivities):
    results = [None] * len(queries)

    # Execute queries on wrapped and store timing
    for i in range(len(queries)):
        if i % 10 == 0:
            print("Finished {}/{} partitions...".format(i, len(queries)))
        query = queries[i].replace("<TABLE>",
                                   "plain" if on_plain else "wrapped")
        run = plain if on_plain else functools.partial(wrapped,
                                                       schema=mapping.schema,
                                                       box=box,
                                                       kv_mode=kvstore)
        times, size = test(query, run)
        results[i] = [i, query, selectivities[i], *size, *times]
    columns = PLAIN_COLUMNS if on_plain else WRAPPED_COLUMNS
    return pd.DataFrame(results, columns=columns)


def test(query, run):
    start = timer()
    result, size, timed = run(engine, query)
    end = timer()
    time_to_return = timed if timed else [end - start]
    size_to_return = size
    if not size_to_return:
        size_to_return = [
            asizeof.asizeof(list(result.to_records(index=False))),
            len(result.index)
        ]
    return time_to_return, size_to_return


def plain(engine, query):
    result = engine.execute(query)
    return pd.DataFrame(result.fetchall(), columns=result.keys()), None, None


# TODO: Avoid making query with empty set of labels
# NOTE: This won't improve our performance evaluation as our queries
# have always a non-empty result
def wrapped(engine, query, schema, box, kv_mode=False):
    rewriting_time = execute_time = decrypt_time = 0
    tuples = []
    if not kv_mode:
        # Rewrite query so that it may be run on the SQL server
        start = timer()
        rewritten, table = rewrite(query, mapping)
        rewriting_time = timer() - start

        # Execute rewritten query on the server
        start = timer()
        result = engine.execute(rewritten)
        rows = result.fetchall()
        execute_time = timer() - start

        # Compute size of the server-side query result in bytes
        bstrings = [bytes(row[0]) for row in rows]
        size = asizeof.asizeof(bstrings)

        # Decrypt the encrypted tuples
        start = timer()
        for row in rows:
            tuples.extend(decrypt(row[0], box))
        decrypt_time = timer() - start
    else:
        # Rewrite query so that it may be run on the key-value store
        start = timer()
        rewritten, table = official_rewrite(query, mapping, kv_store_mode=True)
        rewriting_time = timer() - start

        kv_store_data = rewritten
        if kv_store_data:
            # Execute query on the key-value store
            start = timer()
            if "GroupId" in kv_store_data and len(kv_store_data) == 1:
                pipe = redis_client.pipeline(transaction=False)
                gids = list(kv_store_data["GroupId"])
                for i in range(0, len(gids), CHUNK_SIZE):
                    pipe.hmget(table, gids[i:i + CHUNK_SIZE])
                rows = [row for rows in pipe.execute() for row in rows]
                assert len(rows) == len(gids)
            else:
                # Force GroupId as the first column (when present)
                columns = ["GroupId"] if "GroupId" in kv_store_data else []
                for column in kv_store_data:
                    if column != "GroupId":
                        columns.append(column)
                # Query key-value store using indices
                rows = script(keys=[column for column in columns],
                              args=[
                                  ",".join(map(str, kv_store_data[column]))
                                  for column in columns
                              ])
            execute_time = timer() - start

            # Compute size of the server-side query result in bytes
            size = asizeof.asizeof(rows)

            # Decrypt the encrypted tuples
            start = timer()
            for row in rows:
                tuples.extend(decrypt(row, box))
            decrypt_time = timer() - start

    start = timer()
    df = pd.DataFrame(tuples, columns=schema)
    with sqlite3.connect(':memory:') as conn:
        # Store plaintext tuples in local cache
        df.to_sql(table, conn, if_exists='replace', index=False,
                  method='multi', chunksize=10000)
        create_time = timer() - start

        # Run original query on the local cache
        start = timer()
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [description[0] for description in cursor.description]
        result = pd.DataFrame(cursor.fetchall(), columns=columns)
        filter_time = timer() - start

    # Keep track of the number of encrypted tuples
    nof_enctuples = len(rows)
    # Keep track of the number of the plaintext tuples
    nof_plaintext_tuples = len(tuples)
    # Keep track of the number of tuples in the result
    nof_result_tuples = len(result.index)

    return result, \
           [size, nof_enctuples, nof_plaintext_tuples, nof_result_tuples], \
           [rewriting_time, execute_time, decrypt_time, create_time, filter_time]


def rewrite(query, mapping):
    # NOTE: This is very fragile, as it has a lot of assumtions

    first_column = query.split('WHERE ')[1].split(" ")[0][1:-1]
    labels_first_column = None
    first_was_equality = False
    if "AND" in query:
        if "IN" in query:
            values = eval(query[query.index("(") : query.index(")") + 1])
            labels_first_column = mapping.in_values(first_column, values)
        elif "=" in query and ">=" not in query:
            first_was_equality = True
            value = int(re.match(r"\d+", query.split(" = ")[1]).group())
            labels_first_column = mapping.eq(first_column, value)

    column = first_column if labels_first_column is None else query.split('AND ')[1].split(" ")[0][1:-1]
    labels = None
    if ">=" in query:
        left = int(re.match(r"\d+", query.split(" >= ")[1]).group())
        right  = int(re.match(r"\d+", query.split(" <= ")[1]).group())
        labels = mapping.between(column, (left, right))
    elif "=" in query:
        operand = int(re.match(r"\d+", query.split(" = ")[1 + first_was_equality]).group())
        labels = mapping.eq(column, operand)
    elif "IN" in query:
        values = eval(query[query.index("(") : query.index(")") + 1])
        labels = mapping.in_values(column, values)

    first_column = first_column if not mapping.is_gid(first_column) else "GroupId"
    column = column if not mapping.is_gid(column) else "GroupId"
    if representation == "normal":
        prefix = 'SELECT "EncTuples" FROM wrapped '
    else:
        prefix = 'SELECT "EncTuples" FROM wrapped_with_mapping JOIN mapping USING ("GroupId") '

    if labels_first_column is None:
        rewritten  = prefix + f'INNER JOIN (VALUES {to_string(labels)}) vals(v) ON ("{column}" = v)'
    else:
        rewritten  = prefix + \
                     f'INNER JOIN (VALUES {to_string(labels_first_column)}) vals1(v1) ON ("{first_column}" = v1)' + \
                     f'INNER JOIN (VALUES {to_string(labels)}) vals2(v2) ON ("{column}" = v2)'
    return rewritten, "wrapped"


def to_string(labels):
    if isinstance(next(iter(labels)), str):
        return  ",".join(map(lambda x: "('{}')".format(x), labels))
    return ",".join(map(lambda x: "({})".format(str(x)), labels))


def decrypt(row, box):
    bstring = bytes(row) if not kvstore else row
    try:
        plaintext = box.decrypt(bstring)
    except nacl.exceptions.CryptoError:
        print("ERROR: Something has gone wrong with the decryption of the",
                "tuples.")
        sys.exit()
    lpadding = int.from_bytes(plaintext[:2], byteorder='little', signed=False)
    compressed = plaintext[2:-lpadding] if lpadding else plaintext[2:]
    return deserialize(decompress(compressed))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Query database hosting the wrapped dataset using' +
                    'the given mapping.'
    )
    parser.add_argument('url', metavar='URL',
                        help='URL of the database where the dataset is stored')
    parser.add_argument('queries',
                        metavar='QUERIES', help='path to the list of queries')
    parser.add_argument('output', metavar='OUTPUT',
                        help='path where to store results of the test')
    parser.add_argument('-c',
                        '--compression',
                        metavar='ALGORITHM',
                        choices=['none', 'lz4', 'snappy', 'zstd'],
                        default='zstd',
                        help='compression algorithm: none, lz4, snappy, zstd '
                             '(default)')
    parser.add_argument('-k',
                        '--kvstore',
                        action='store_true',
                        help='use key-value store')
    parser.add_argument('-m',
                        '--mapping',
                        metavar='MAPPING', help='path to the mapping')
    parser.add_argument('--password',
                        help='password necessary to read the mapping')
    parser.add_argument('-p',
                        '--plain',
                        action='store_true',
                        help='evaluate performance on the plain dataset')
    parser.add_argument('-r',
                        '--representation',
                        metavar='REPRESENTATION',
                        default="normal",
                        help='server-side representation of the dataset: ' +
                             'normal (default), mapping')
    parser.add_argument('-s',
                        '--sample-size',
                        metavar='SAMPLE_SIZE',
                        type=int,
                        default=1000,
                        help='size of the sample of queries to pick')
    parser.add_argument('--serialization',
                        metavar='FORMAT',
                        choices=['json', 'msgpack', 'pickle'],
                        default='json',
                        help='serialization format: json (default), msgpack, '
                             'pickle')
    parser.add_argument('-t',
                        '--type',
                        metavar='TYPE',
                        default="heterogeneous",
                        help='type of mapping: heterogeneous (default)')
    args = parser.parse_args()

    # Mandatory parameters
    url = args.url
    queries = args.queries
    output = args.output

    # Optional flags and parameters
    decompress = DECOMPRESS[args.compression]
    kvstore = args.kvstore
    path = args.mapping
    pw = args.password.encode("utf-8") if args.password else None
    on_plain = args.plain
    representation = args.representation
    sample_size = args.sample_size
    deserialize = DESERIALIZE[args.serialization]
    type = args.type

    if type not in MAPPINGS:
        parser.error(f"{type} is not a valid mapping type.")

    if representation not in ("normal", "mapping"):
        parser.error(
            f"{repr} is not a valid server-side representation of the dataset."
        )

    if on_plain and (path or pw):
        parser.error("Performance evaluation on the plain dataset does not " +
                     "need a mapping, nor a password.")

    if on_plain and representation != "normal":
        parser.error("Performance evaluation on the plain dataset only " +
                     "supports normal representation.")

    if not on_plain and not path:
        parser.error("Performance evaluation on wrapped dataset needs a " +
                     "mapping")

    engine = None
    if not kvstore:
        engine = sqlalchemy.create_engine(url)
    else:
        host, port = url.split(":")
        redis_client = redis.Redis(host=host, port=port)
        root = os.path.realpath(os.path.join(__file__, "..", "..", ".."))
        script_path = os.path.join(root, "redis", "indices.lua")
        with open(script_path) as script_file:
            script = redis_client.register_script(script_file.read())

    if path:
        # Read encrypted range mapping
        if not pw:
            pw = getpass.getpass("Password: ").encode("utf-8")
        salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'
        kdf = nacl.pwhash.argon2id.kdf
        key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)
        box = nacl.secret.SecretBox(key)
        mapping = MAPPINGS[type](path, key)

    print("[*] Evaluate performance of queries")
    df = pd.read_csv(queries)
    if len(df.index) > sample_size:
        df = df.sample(sample_size, random_state=0)
    df = evaluate(list(df["query"]), list(df["size"]))

    print("[*] Write performance of queries")
    df.to_csv(output, index=False)
