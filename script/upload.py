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

import pandas as pd
import redis
import sqlalchemy
from sqlalchemy import create_engine

from secure_index.mapping._column_mapping.creation import is_set, get_items

# TODO: Assumption to handle, the anonymized non-wrapped table always presents the INDEX and GID columns
# TODO: Assumption to handle, the wrapped table always presents the GroupId column when whe wrapped_id table is created

def wrapped_id_table(engine, name, adf):
    """ Create the auxiliary table necessary to perform queries on the ID
    attribute of the original table
    :engine: sql engine used to perform DB operations
    :name: name of the referenced table
    :adf: anonymized dataframe containing the relation between ID and GID
    """

    command = f'CREATE TABLE wrapped_id("INDEX" bigint, "GID" bigint, PRIMARY KEY("INDEX"), CONSTRAINT fk_gid FOREIGN KEY("GID") REFERENCES "{name}"("GroupId"))'
    engine.execute(command)
    adf.to_sql('wrapped_id', engine, if_exists='append', index=False)

parser = argparse.ArgumentParser(
    description='Run examples against the given mapping.'
)
parser.add_argument('dataset',
                    metavar='PLAIN',
                    help='path to the plain dataset')

parser.add_argument('url',
                    metavar='URL',
                    help='URL of the DBMS or KV-STORE where to store the ' +
                         'dataset')
parser.add_argument('--kvstore',
                    dest='kvstore',
                    action='store_true',
                    help='use a kv-store as target')
parser.add_argument('-n',
                    '--name',
                    metavar='NAME',
                    help='name of data structure where to store the dataset')
parser.add_argument('-a',
                    '--anonymized-dataset',
                    metavar='ANONIMIZED_PATH',
                    dest="anonymized_path",
                    help='path to the non wrapped anonymized dataset')

args = parser.parse_args()

dataset = args.dataset
url = args.url
kvstore = args.kvstore
name = args.name if args.name is not None else "wrapped"
anon_path = args.anonymized_path if args.anonymized_path is not None else None

print(f"[*] Read {dataset} dataset")
df = pd.read_csv(dataset)

# Reduce in-memory footprint of the indices columns
for column in df.columns:
    if df[column].dtype == "int64":
        df[column] = pd.to_numeric(df[column], downcast="signed")

if not kvstore:
    print(f"[*] Upload {dataset} as {name} table")
    engine = create_engine(url)
    engine.execute("DROP TABLE IF EXISTS wrapped_id")

    # Ensure sqlalchemy treats EncTuples as bytes
    if "EncTuples" in df.columns:
        df["EncTuples"] = df["EncTuples"].apply(base64.b64decode)

    # Use memory efficient types to store the dataset server-side
    dtype = {}
    for column in df.columns:
        if column == "EncTuples":
            dtype[column] = sqlalchemy.dialects.postgresql.BYTEA
        elif str(df[column].dtype).startswith("int"):
            if df[column].dtype in ("int8", "int16"):
                dtype[column] = sqlalchemy.dialects.postgresql.SMALLINT
            elif df[column].dtype == "int32":
                dtype[column] = sqlalchemy.dialects.postgresql.INTEGER
            elif df[column].dtype == "int64":
                dtype[column] = sqlalchemy.dialects.postgresql.BIGINT
        else:
            # TODO: Consider treating hashes as bytea instead of text
            dtype[column] = sqlalchemy.dialects.postgresql.TEXT

    df.to_sql(name,
              engine,
              dtype=dtype,
              if_exists='replace',
              index=False)

    # TODO: Create referential constraints on columns (make sense only when
    #       representing the dataset with a mapping and in normal form)
    # TODO: Improve index creation considering query pattern (current one
    #       is wrong only when representing the dataset with a mapping and in
    #       normal form)

    if "INDEX" in df.columns:
        primary_key = ["INDEX"]
    elif "GroupId" in df.columns:
        primary_key = ["GroupId"]
    elif "Id" in df.columns:
        primary_key = ["Id"]
    else:
        primary_key = [
            column for column in df.columns if column != "EncTuples"
        ]

    unique = not df.duplicated(subset=primary_key).any()
    if unique:
        string = '\",\"'.join(primary_key)
        print(f"[*] Create primary key on {name} using \"{string}\" columns")
        engine.execute(f"ALTER TABLE \"{name}\" ADD PRIMARY KEY (\"{string}\")")

    # Create single column index on column indices
    # TODO: Verify the following note
    # NOTE: our queries never use single column indexes due to the huge number
    #       of punctual values we request (this may change on multi-column
    #       queries)
    for column in df.columns:
        if column not in ("INDEX", "GroupId", "Id", "EncTuples"):
            # Depending on the uniqueness of the column create a unique or a
            # normal index
            is_unique = df[column].nunique() == len(df.index)
            string = 'UNIQUE' if is_unique else ''
            print(f"[*] Create {string} index on {name} using {column}")
            engine.execute(f"CREATE {string} INDEX \"{name}_{column}_idx\" " +
                           f"ON \"{name}\" (\"{column}\")")

    # Create multi column index on column indices (according to test query
    # pattern)
    multi_column_index = [
        column for column in df.columns
        if column not in ("INDEX", "GroupId", "Id", "EncTuples")
    ]
    if len(multi_column_index) > 1:
        is_unique = not df.duplicated(subset=multi_column_index).any()
        unique_string = 'UNIQUE' if is_unique else ''
        # Our experimental evaluation runs query against WAGP, and WAGP+OCCP,
        # as the column order of the usa2019 dataset is ST, AGEP, OCCP and WAGP
        # we reverse it
        multi_column_index.reverse()
        columns_string = '\",\"'.join(multi_column_index)
        print(f"[*] Create {unique_string} index on {name} using " +
              f"\"{columns_string}\"")
        engine.execute(f"CREATE {string} INDEX \"{name}_multi_column_idx\" " +
                       f"ON \"{name}\" (\"{columns_string}\")")

    if anon_path is not None:
        adf = pd.read_csv(anon_path)
        wrapped_id_table(engine, name, adf[["INDEX", "GID"]])
else:
    print(f"[*] Upload {dataset} as {name} hash")
    host, port = url.split(":")
    r = redis.Redis(host=host, port=port)

    is_mapping = df['Value'].apply(is_set).all()
    if is_mapping:
        pipe = r.pipeline(transaction=False)
        for key, value in zip(df['Key'], df['Value']):
            pipe.sadd(name + ":" + str(key), *map(str.strip, get_items(value)))
        pipe.execute()
    else:
        # Ensure Redis treats EncTuples as bytes
        df["Value"] = df["Value"].apply(base64.b64decode)

        #r.mset({key: value for key, value in zip(df['Key'], df['Value'])})
        r.hset(name,
               mapping={key: value
                        for key, value in zip(df['Key'], df['Value'])})
