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


def to_string(labels):
    if isinstance(next(iter(labels)), str):
        return  ",".join(map(lambda x: "('{}')".format(x), labels))
    return ",".join(map(lambda x: "({})".format(str(x)), labels))


def rewrite(query, mapping):
    labels = None
    column = query.split('WHERE ')[1].split(" ")[0][1:-1]
    if(">=" in query):
        left = int(re.match(r"\d+", query.split(" >= ")[1]).group())
        right  = int(re.match(r"\d+", query.split(" <= ")[1]).group())
        labels = mapping.between(column, (left, right))
    else:
        operand = int(re.match(r"\d+", query.split(" = ")[1]).group())
        labels = mapping.eq(column, operand)

    column = column if not mapping.is_gid(column) else "GroupId"
    if representation == "normal":
        prefix = 'SELECT "EncTuples" FROM wrapped '
    else:
        prefix = 'SELECT "EncTuples" FROM wrapped_with_mapping JOIN mapping USING ("GroupId") '
    rewritten  = prefix + f'INNER JOIN (VALUES {to_string(labels)}) vals(v) ON ("{column}" = v)'
    return rewritten, "wrapped"


def test(query, run):
    start = timer()
    result, timed = run(engine, query)
    end = timer()
    time_to_return = timed if timed else end - start
    return time_to_return, len(result)


def plain(engine, query):
    result = engine.execute(query)
    return pd.DataFrame(result.fetchall(), columns=result.keys()), None


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
        return result, [rewriting_time, execute_time, decrypt_time, create_time, filter_time]


def decrypt(row, box):
    bstring = bytes(row) if kvstore_url is None else row
    try:
        plaintext = box.decrypt(bstring)
    except nacl.exceptions.CryptoError:
        print("ERROR: Something has gone wrong with the decryption of the",
                "tuples.")
        sys.exit()
    lpadding = int.from_bytes(plaintext[:2], byteorder='little', signed=False)
    compressed = plaintext[2:-lpadding] if lpadding else plaintext[2:]
    return deserialize(decompress(compressed))


PLAIN_COLUMNS = ["index", "query", "size", "plain"]

WRAPPED_COLUMNS = [
    "index", "query", "size", "rewriting", "server", "decryption", "creation",
    "filtering"
]


def evaluate_plain(query, params):
    results = [None] * len(params)

    # Execute queries on the plain dataset
    for index, param in enumerate(params):
        if index % 10 == 0:
            print("Finished {}/{} partitions...".format(index, len(params)))
        time, size = test(query % ("plain", *param), plain)
        results[index] = [
            index, query % ("plain", *param), size / cardinality, time
        ]
    return pd.DataFrame(results, columns=PLAIN_COLUMNS)


def evaluate_wrapped(query, params, baseline=None):
    # Retrieve sizes of the baseline results
    df_baseline = pd.read_csv(baseline, index_col="index")
    sizes = df_baseline["size"]

    results = [None] * len(params)

    # Execute queries on wrapped and store timing
    for index, param in enumerate(params):
        if index % 10 == 0:
            print("Finished {}/{} partitions...".format(index, len(params)))

        # Keep query requiring less than 30% of the dataset
        if sizes.iloc[index] > 0.3:
            continue

        times, size = test(query % ("wrapped", *param),
                           functools.partial(wrapped,
                                             schema=mapping.schema,
                                             box=box,
                                             kv_mode=kvstore_url is not None))
        results[index] = [
            index, query % ("wrapped", *param), size / cardinality, *times
        ]

    # Drop rusults corresponding to skipped queries
    for index in range(len(results) - 1, -1, -1):
        if results[index] is None:
            del results[index]

    return pd.DataFrame(results, columns=WRAPPED_COLUMNS)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Query database hosting the wrapped dataset using' +
                    'the given mapping.'
    )
    parser.add_argument('url', metavar='URL',
                        help='URL of the database where the dataset is stored')
    parser.add_argument('column', metavar='COLUMN', help='column name to query')
    parser.add_argument('output', metavar='OUTPUT',
                        help='directory where to store results of the test')
    parser.add_argument('-b',
                        '--baseline',
                        metavar='PATH',
                        help='path to the performance evaluation baseline')
    parser.add_argument('-c',
                        '--compression',
                        metavar='ALGORITHM',
                        choices=['none', 'lz4', 'snappy', 'zstd'],
                        default='zstd',
                        help='compression algorithm: none, lz4, snappy, zstd '
                             '(default)')
    parser.add_argument('-k',
                        '--kvstore',
                        metavar='KVSTORE_URL',
                        help='URL of the kv store where the dataset is stored')
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
                        default='normal',
                        help='server-side representation of the dataset: ' +
                             'normal (default), mapping')
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

    url = args.url
    path = args.mapping
    column = args.column
    output = args.output
    type = args.type
    on_plain = args.plain
    baseline = args.baseline
    kvstore_url = args.kvstore
    representation = args.representation
    pw = args.password.encode("utf-8") if args.password else None
    deserialize = DESERIALIZE[args.serialization]
    decompress = DECOMPRESS[args.compression]

    if type not in MAPPINGS:
        parser.error(f"{type} is not a valid mapping type.")

    if representation not in ("normal", "mapping"):
        parser.error(
            f"{repr} is not a valid server-side representation of the dataset."
        )

    if on_plain and (baseline or path):
        parser.error("Performance evaluation on the plain dataset does not " +
                     "need a baseline nor a mapping.")

    if not on_plain and (not baseline or not path):
        parser.error("Performance evaluation on wrapped dataset needs a " +
                     "a baseline and a mapping")

    # Pick evaluate function according what we want to evaluate
    evaluate = evaluate_plain if on_plain else evaluate_wrapped

    # Connect to database
    engine = sqlalchemy.create_engine(url)

    if kvstore_url:
        # Connect to kv store
        host, port = kvstore_url.split(":")
        redis_client = redis.Redis(host=host, port=port)
        root = os.path.realpath(os.path.join(__file__, "..", "..", ".."))
        script_path = os.path.join(root, "redis", "indices.lua")
        with open(script_path) as script_file:
            script = redis_client.register_script(script_file.read())

    # # Create indexes
    # for table in ['plain', 'wrapped']:
    #     print(f"[*] Create index on {table} using {column}")
    #     engine.execute(
    #         f"CREATE INDEX \"{table}_{column}_idx\" ON \"{table}\" " +
    #         f"USING hash (\"{column}\");"
    #     )

    if path:
        # Read encrypted range mapping
        if not pw:
            pw = getpass.getpass("Password: ").encode("utf-8")
        salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'
        kdf = nacl.pwhash.argon2id.kdf
        key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)
        box = nacl.secret.SecretBox(key)
        mapping = MAPPINGS[type](path, key)

    # Number of tuples of the dataset
    result = engine.execute("SELECT COUNT(*) FROM plain")
    cardinality = result.fetchone()[0]

    # Get unique values of the given column
    result = engine.execute(
        f"SELECT DISTINCT \"{column}\" FROM plain ORDER BY \"{column}\""
    )
    values = pd.DataFrame(result.fetchall(), columns=[column])

    print("[*] Evaluate performance of punctual queries")
    query = f"SELECT * FROM %s WHERE \"{column}\" = %s"
    params = [(value,) for _, value in values[column].items()]
    if baseline:
        punctual_baseline = os.path.join(baseline, "punctual.csv")
        evaluate = functools.partial(evaluate,
                                     baseline=punctual_baseline)
    df = evaluate(query, params)

    print("[*] Write performance of punctual queries")
    df.to_csv(os.path.join(output, "punctual.csv"), index=False)

    print("[*] Evaluate performance of range queries")
    query = f"SELECT * FROM %s WHERE \"{column}\" >= %s AND \"{column}\" <= %s"
    params = []
    for i, value in values[column].items():
        for j in range(i + 1, len(values)):
            params.append((value, values[column].iloc[j]))
    if baseline:
        range_baseline = os.path.join(baseline, "range.csv")
        evaluate = functools.partial(evaluate,
                                     baseline=range_baseline)
    df = evaluate(query, params)

    print("[*] Write performance of range queries")
    df.to_csv(os.path.join(output, "range.csv"), index=False)
