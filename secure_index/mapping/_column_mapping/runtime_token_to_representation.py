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

import functools
import math
import multiprocessing

from Crypto.Cipher import AES


BLOCK_SIZE = 16 # bytes
TOKEN_SIZE = 8 # bytes
TOKENS_PER_BLOCK = BLOCK_SIZE / TOKEN_SIZE


def get_token_representations(token, key, salt):
    """Runtime util to get all the representations of a single token.  

    Args: 

        token: the token from which to generate the representations. A
        token is a tuple storing the token value (integer) and its
        frequency (integer) expected.

        key: the master key used to encrypt the database (we might
        consider using two different keys: 1 for the blocks the other
        one for the indices). 16 or 32 bytes expected.

        salt: the salt related to the current index column. 16 bytes
        expected.

    Returns:

        A set of all representations. Each representation is a 64 bit
        integer. Collisions are possible, but unlikely to be produced.

    """
    starting_token, nof_tokens = token

    # Initialize the block with the starting token
    block = starting_token.to_bytes(BLOCK_SIZE, byteorder="big")
    nof_blocks = math.ceil(nof_tokens / TOKENS_PER_BLOCK)

    # Initialize the memory as multiple blocks
    memory = bytearray(block * nof_blocks)

    cipher = AES.new(key, AES.MODE_CBC, IV=salt)
    enc = cipher.encrypt(memory)

    # Retrieve list of tokens from the encrypted memory
    return [
        int.from_bytes(enc[i * TOKEN_SIZE:(i + 1) * TOKEN_SIZE], "big") >> 1
        for i in range(nof_tokens)
    ]


def get_all_tokens_representations(tokens, key, salt):
    """"Runtime util to get all the representations of all the tokens related to a colum.

    Args: 

        tokens: a list of tokens from which to generate the
        representations. Integer expected.

        key: the master key used to encrypt the database (we might
        consider using two different keys: 1 for the blocks the other
        one for the indices). 16 bytes expected.

        salt: the salt related to the current index column. 16 bytes
        expected.

    Returns:

        A list of all representations (order matters). Each
        representation is a 64 bit integer. Collisions are possible,
        but unlikely to be produced.

    """
    get_representations = functools.partial(get_token_representations,
                                            key=key,
                                            salt=salt)

    with multiprocessing.Pool() as pool:
        representations = pool.map(get_representations, tokens)
    return list(representations)
