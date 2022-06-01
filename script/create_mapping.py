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
import getpass
import json
import os
import pickle
import sys

import nacl.encoding
import nacl.hash
import nacl.pwhash
import nacl.secret
import nacl.utils
import pandas as pd

from secure_index.mapping.creation import create_heterogeneous_mapping


TYPES = {
    "bitmap",
    "interval-tree",
    "range",
    "roaring",
    "set",
}

parser = argparse.ArgumentParser(
    description='Create a mapping on the k-anonymous dataset.'
)
parser.add_argument('input',
                    metavar='INPUT',
                    help='path to the k-anonimous dataset')
parser.add_argument('output',
                    metavar='OUTPUT',
                    help='path where to store the mapping')
parser.add_argument('-c',
                    '--column',
                    metavar='COLUMN',
                    help='name of the column to create the mapping for')
parser.add_argument('-e',
                    '--enc',
                    dest='to_enc',
                    action='store_true',
                    help='encrypt the mapping at rest')
parser.add_argument('-g',
                    '--gid',
                    dest='to_gid',
                    action='store_true',
                    help='use group ids to wrap generalization strings')
parser.add_argument('--hash',
                    dest='to_hash',
                    action='store_true',
                    help='use hash of the generalization strings')
parser.add_argument('-p',
                    '--plain',
                    dest='to_keep_plain',
                    action='store_true',
                    help='use plain generalization strings')
parser.add_argument('--password',
                    help='password necessary to read the mapping')
parser.add_argument('-r',
                    '--runtime',
                    dest='to_runtime',
                    action='store_true',
                    help='use runtime tokens generation')
parser.add_argument('-t',
                    '--type',
                    metavar='TYPE',
                    help='either a type of mapping among range (default), ' +
                         'interval-tree, bitmap, roaring and set, or a path ' +
                         'to a JSON file containing an heterogeneous ' +
                         'mapping configuration')

args = parser.parse_args()
dataset = args.input
destination = args.output
column = args.column
to_enc = args.to_enc
to_gid = args.to_gid
to_hash = args.to_hash
to_keep_plain = args.to_keep_plain
to_runtime = args.to_runtime
type = args.type if args.type else "range"
pw = args.password.encode("utf-8") if args.password else None

if type not in TYPES and not os.path.isfile(type):
    parser.error(
        f"{type} is not a valid mapping type nor a valid mapping " +
        "configuration file."
    )

if to_gid + to_hash + to_keep_plain + to_runtime > 1:
    parser.error(
        "Only one flag among --gid, --hash, --plain and --runtime can be set."
    )

# Ask user to insert password when we need to derive a key due to either
# the encryption of the mapping at rest or computing hashes
key = None
if to_enc or to_hash:
    if not pw:
        try:
            pw = getpass.getpass("Password: ").encode("utf-8")
            confirm = getpass.getpass("Confirm password: ").encode("utf-8")
        except UnicodeError:
            raise RuntimeError("Only utf-8 compatible passwords allowed")
        if pw != confirm:
            print("ERROR: Your password and confirmation password do not match.")
            sys.exit()

    salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'

    # Generate the key
    kdf = nacl.pwhash.argon2id.kdf
    key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)

print("[*] Read anonymized dataset")
df = pd.read_csv(dataset, dtype=object)

print("[*] Remove duplicates to speed up mapping creation")
df.drop_duplicates("GID", inplace=True)

columns = [column for column in df.columns if column not in ["INDEX", "GID"]]
columns_to_map = [column] if column else columns

# Construct an heterogeneous mapping config
if type not in TYPES:
    with open(type) as config:
        configs = json.load(config)
else:
    config = {
        "type": type,
        "plain": to_keep_plain,
        "hash": to_hash,
        "gid": to_gid,
        "runtime": to_runtime
    }
    configs = {column: config for column in columns_to_map}

# Create heterogeneous mapping
mapping = create_heterogeneous_mapping(df, configs, key)

# Store schema information within metadata
metadata = (tuple(columns), mapping)

# Write mapping to file
if to_enc:
    # Produce pickled representation of the metadata as a bytes object
    plaintext = pickle.dumps(metadata)

    # Encrypt metadata
    box = nacl.secret.SecretBox(key)
    encrypted = box.encrypt(plaintext)

    # Store encrypted metadata
    with open(destination, 'w') as f:
        content = base64.b64encode(encrypted).decode("ascii")
        f.write(content)
else:
    # Write pickled representation of the metadata to file in plaintext
    with open(destination, 'wb') as output:
        pickle.dump(metadata, output)
