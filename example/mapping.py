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

import nacl.pwhash
import nacl.secret

from secure_index.mapping.heterogeneous import HeterogeneousMapping

MAPPINGS = {
    "heterogeneous": HeterogeneousMapping,
}

parser = argparse.ArgumentParser(
    description='Run examples against the given mapping.'
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
args = parser.parse_args()
path = args.mapping
to_enc = args.to_enc
type = args.type if args.type else "heterogeneous"

if type not in MAPPINGS:
    parser.error(f"{type} is not a valid mapping type.")

if to_enc:
    pw = getpass.getpass("Password: ").encode("utf-8")
    salt = b'\xd0\xe1\x03\xc2Z<R\xaf]\xfe\xd5\xbf\xf8u|\x8f'
    # Generate the key
    kdf = nacl.pwhash.argon2id.kdf
    key = kdf(nacl.secret.SecretBox.KEY_SIZE, pw, salt)

    mapping = MAPPINGS[type](path, key)
else:
    mapping = MAPPINGS[type](path)

# USA 2018 (500k)
print("[*] AGE = 18")
print(mapping.eq("AGE", 18))

print("[*] AGE <> 18")
print(mapping.neq("AGE", 18))

print("[*] AGE IN (10, 18, 20, 80, 100)")
print(mapping.in_values("AGE", [10, 18, 20, 80, 100]))

print("[*] AGE <= 18")
print(mapping.le("AGE", 18))

print("[*] AGE >= 18")
print(mapping.ge("AGE", 18))

print("[*] AGE BETWEEN 75 AND 79")
print(mapping.between("AGE", (75, 79)))

# # USA 2019 (3.5M)
# print("[*] AGEP <= 18")
# print(mapping.le("AGEP", 18))

# print("[*] AGEP = 18")
# print(mapping.eq("AGEP", 18))

# print("[*] AGEP >= 18")
# print(mapping.ge("AGEP", 18))

# print("[*] AGEP BETWEEN 75 AND 79")
# print(mapping.between("AGEP", (75, 79)))
