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
import getpass
import json
import os
import pickle
import sqlite3
import sys
from functools import partial

import lz4.frame
import msgpack
import nacl.pwhash
import nacl.secret
import pandas as pd
import snappy
import sqlalchemy
import redis
import zstd

from secure_index.mapping.heterogeneous import HeterogeneousMapping
from secure_index.rewriting import rewrite
from secure_index.rewriting import rewrite_table_with_mapping
from secure_index.rewriting import rewrite_table_with_normalization


CHUNK_SIZE= 10000

MAPPINGS = {
    "heterogeneous": HeterogeneousMapping,
}

TABLES = {
    "normal": "wrapped",
    "mapping": "wrapped_with_mapping",
    "normalization": "wrapped_with_normalization"
}

REWRITE_TABLES = {
    "normal": None,
    "mapping": rewrite_table_with_mapping,
    "normalization": rewrite_table_with_normalization
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


def test(query):
    print(f"\n[*] {query}")
    result = execute(query, mapping, rewrite_table, box)
    print("\n", result, sep="")


def execute(query, mapping, rewrite_table, box):

    # Rewrite query so that it may be run on the server
    rewritten, table = rewrite(query,
                               mapping,
                               rewrite_table=rewrite_table,
                               kv_store_mode=kvstore)

    print("\n", rewritten, sep="")

    tuples = []
    if not kvstore:
        # Execute rewritten query on the server
        result = engine.execute(rewritten)
        rows = result.fetchall()

        # Decrypt the encrypted tuples
        for row in rows:
            tuples.extend(decrypt(row[0]))
    else:
        # Execute query on the key-value store
        kv_store_data = rewritten
        if kv_store_data:
            # Query key-value store
            if "GroupId" in kv_store_data and len(kv_store_data) == 1:
                pipe = engine.pipeline(transaction=False)
                gids = list(kv_store_data["GroupId"])
                for i in range(0, len(gids), CHUNK_SIZE):
                    #pipe.mget(gids[i:i + CHUNK_SIZE])
                    pipe.hmget(table, gids[i:i + CHUNK_SIZE])
                rows = [row for rows in pipe.execute() for row in rows]
                assert len(rows) == len(gids)
            else:
                # Forse GroupId as the first column (when present)
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

            # Decrypt the encrypted tuples
            for row in rows:
                tuples.extend(decrypt(row))

    df = pd.DataFrame(tuples, columns=mapping.schema)

    with sqlite3.connect(':memory:') as conn:
        # Store plaintext tuples in local cache
        df.to_sql(table, conn, if_exists='replace', index=False,
                  method='multi', chunksize=10000)

        # Run original query on the local cache
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(cursor.fetchall(), columns=columns)


def decrypt(row):
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
    parser.add_argument('input', metavar='INPUT', help='path to the mapping')
    parser.add_argument('url', metavar='URL',
                        help='URL of the database or the kv store where the '
                             'dataset is stored')
    parser.add_argument('-c',
                        '--compression',
                        metavar='ALGORITHM',
                        choices=['none', 'lz4', 'snappy', 'zstd'],
                        default='zstd',
                        help='compression algorithm: none, lz4, snappy, zstd '
                             '(default)')
    parser.add_argument('-k',
                        '--kvstore',
                        dest='kvstore',
                        action='store_true',
                        help='prepare the files with the kv-store as the '
                             'target')
    parser.add_argument('--password',
                        help='password necessary to read the mapping')
    parser.add_argument('-r',
                        '--representation',
                        metavar='REPRESENTATION',
                        default='normal',
                        help='server-side representation of the dataset: '
                             'normal (default), mapping, normalization')
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
    path = args.input
    url = args.url
    type = args.type
    repr = args.representation
    kvstore = args.kvstore
    pw = args.password.encode("utf-8") if args.password else None
    deserialize = DESERIALIZE[args.serialization]
    decompress = DECOMPRESS[args.compression]

    if type not in MAPPINGS:
        parser.error(f"{type} is not a valid mapping type.")

    if repr not in REWRITE_TABLES:
        parser.error(
            f"{repr} is not a valid server-side representation of the dataset."
        )

    table = TABLES[repr]
    rewrite_table = REWRITE_TABLES[repr]

    if not pw:
        pw = getpass.getpass("Password: ").encode("utf-8")
    salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'
    # Generate the key
    kdf = nacl.pwhash.argon2id.kdf
    key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)
    box = nacl.secret.SecretBox(key)

    mapping = MAPPINGS[type](path, key)

    # retrieve the proper target
    if not kvstore:
        # Connect to database
        engine = sqlalchemy.create_engine(url)
    else:
        # Connect to kv store
        host, port = url.split(":")
        engine = redis.Redis(host=host, port=port)
        root = os.path.realpath(os.path.join(__file__, "..", ".."))
        script_path = os.path.join(root, "redis", "indices.lua")
        with open(script_path) as script_file:
            script = engine.register_script(script_file.read())

    print("[*] Run some test query")
    test(f"SELECT * FROM {table} WHERE {table}.\"AGE\" = 18")
    test(f"SELECT * FROM {table} WHERE \"{table}\".\"AGE\" = 18")
    test(f"SELECT * FROM {table} WHERE \"AGE\" IN (18, 30, 95)")
    test(f"SELECT \"AGE\", \"STATEFIP\", \"OCC\" FROM {table}" +
            " WHERE \"AGE\"<=18")
    test(f"SELECT COUNT(*) FROM \"{table}\" WHERE 18 >= \"AGE\" /*comment*/")
    test(f"SELECT COUNT(*) FROM {table} WHERE \"AGE\">200-- other comment")
    test(f"SELECT COUNT(*) FROM \"{table}\"" +
            " WHERE \"AGE\"<=18 AND \"STATEFIP\"=55")
    test(f"SELECT COUNT(*) FROM {table} WHERE \"AGE\" <20 GROUP BY \"AGE\"")
    test(f"SELECT COUNT(*) FROM {table} WHERE 20>\"AGE\"" +
            " GROUP BY \"AGE\" HAVING COUNT(*) > 10")
    test(f"SELECT COUNT(*) FROM {table} WHERE 90< \"AGE\" ORDER BY \"AGE\"")
    #test(f"SELECT * FROM {table} WHERE \"AGE\" BETWEEN 18 AND 20")
