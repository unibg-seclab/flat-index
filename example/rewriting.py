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

import nacl.pwhash
import nacl.secret

from secure_index.mapping.heterogeneous import HeterogeneousMapping
from secure_index.rewriting import rewrite
from secure_index.rewriting import rewrite_comparisons
from secure_index.rewriting import rewrite_table_with_mapping
from secure_index.rewriting import rewrite_table_with_normalization


MAPPINGS = {
    "heterogeneous": HeterogeneousMapping,
}

REWRITE_TABLES = {
    "normal": None,
    "mapping": rewrite_table_with_mapping,
    "normalization": rewrite_table_with_normalization
}


def test(query):
    print(f"\n[*] {query}")
    rewritten, table = rewrite(query, mapping,
                               rewrite_table=rewrite_table,
                               rewrite_comparisons=rewrite_comparisons)
    print(rewritten)


parser = argparse.ArgumentParser(
    description='Rewrite queries using the given mapping.'
)
parser.add_argument('mapping', metavar='MAPPING', help='path to the mapping')
parser.add_argument('-e',
                    '--enc',
                    dest='to_enc',
                    action='store_true',
                    help='treat the mapping as encrypted at rest')
parser.add_argument('-t',
                    '--type',
                    metavar='TYPE',
                    help='type of mapping: heterogeneous (default)')
parser.add_argument('-r',
                    '--representation',
                    metavar='REPRESENTATION',
                    help='server-side representation of the dataset: normal ' +
                         '(default), mapping, normalization')
args = parser.parse_args()
path = args.mapping
to_enc = args.to_enc
type = args.type if args.type else "heterogeneous"
repr = args.representation if args.representation else "normal"

if type not in MAPPINGS:
    parser.error(f"{type} is not a valid mapping type.")

if repr not in REWRITE_TABLES:
    parser.error(
        f"{repr} is not a valid server-side representation of the dataset."
    )

if to_enc:
    pw = getpass.getpass("Password: ").encode("utf-8")
    salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'
    # Generate the key
    kdf = nacl.pwhash.argon2id.kdf
    key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)

    mapping = MAPPINGS[type](path, key)
else:
    mapping = MAPPINGS[type](path)

rewrite_table = REWRITE_TABLES[repr]

test(f"SELECT * FROM wrapped")
test("SELECT * FROM wrapped WHERE wrapped.\"AGE\" = 18")
test("SELECT * FROM wrapped WHERE \"wrapped\".\"AGE\" = 18")
test("SELECT \"AGE\", \"STATEFIP\", \"OCC\" FROM wrapped WHERE \"AGE\"<=18")
test("SELECT COUNT(*) FROM \"wrapped\" WHERE 18 >= \"AGE\" /*comment*/")
test("SELECT COUNT(*) FROM wrapped WHERE \"AGE\">200-- other comment")
test("SELECT COUNT(*) FROM \"wrapped\" WHERE \"AGE\"<=18 AND \"STATEFIP\"=12")
test("SELECT COUNT(*) FROM wrapped WHERE \"AGE\" <20 GROUP BY \"AGE\"")
test("SELECT COUNT(*) FROM wrapped WHERE 20>\"AGE\" " +
     "GROUP BY \"AGE\" HAVING COUNT(*) > 10")
test("SELECT COUNT(*) FROM wrapped WHERE 90< \"AGE\" ORDER BY \"AGE\"")
#test("SELECT * FROM wrapped WHERE \"AGE\" IN (18, 30, 95)")
#test("SELECT * FROM wrapped WHERE \"AGE\" BETWEEN 18 AND 20")
