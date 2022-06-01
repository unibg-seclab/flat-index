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
import csv
import itertools
import math
import os
import re
import subprocess
import time
from Crypto.Cipher import AES


""" USAGE
./test_token_collisions.py --token-sizes 1 2 3 4 --token-numbers 1 2 3 4 5 6 7 8 9 10 20 30 40 50 60 70 80 90 100 200 300 400 500 600 700 800 900 1000 2000 3000 4000 5000 6000 7000 8000 9000 10000 20000 30000 40000 50000 60000 70000 80000 90000 100000 200000 300000 400000 500000 600000 700000 800000 900000 1000000 2000000 3000000 4000000 5000000 6000000 7000000 8000000 9000000 10000000 20000000 30000000 40000000 50000000 60000000 70000000 80000000 90000000"""

DEBUG = False

starting_token = 1 # integer required
master_key = b'yellow submarine'
salt = b'yellow submarine'
nof_blocks = 0
tokens = set()
collisions = 0

parser = argparse.ArgumentParser(
    description='Test runtime token generation performance and counts the number of collisions.'
)
parser.add_argument('-r',
                    '--recap',
                    dest='recap',
                    action='store_true',
                    help='print memory to output')
parser.add_argument('-s',
                    '--token-sizes',
                    nargs='+',
                    type=int,
                    default=[4],
                    metavar='TOKEN-SIZE',
                    dest='token_sizes',
                    help='list of token sizes in bytes (default: 4 bytes)')
parser.add_argument('-n',
                    '--token-numbers',
                    nargs='+',
                    type=int,
                    default=[1000000],
                    metavar='TOKEN-NUMBER',
                    dest='frequencies',
                    help='list of number of tokens to generate (default: 1000000)')

"""TEST CONFIGURATION"""
args = parser.parse_args()
inspect_output = args.recap
token_sizes = args.token_sizes
token_frequencies = args.frequencies

"""TIME MEASURES"""
tot_test_time = 0
tot_mem_create_time = 0
tot_enc_time = 0
tot_token_extraction_time = 0
nof_tokens = 0
tot_mem_size = 0


"""TEST"""
test = os.path.realpath(os.path.join(__file__, "..", ".."))
log_path = os.path.join(test, "results", "collision", "simulation.csv")

start_time = time.time()
combinations = itertools.product(token_sizes, token_frequencies)
BLOCK_SIZE = 16 # bytes
token_frequency = 0

with open(log_path, mode='w') as logfile:
    logfile_writer = csv.writer(logfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    logfile_writer.writerow(['Token_size[Bytes]', 'Token_frequency', 'Collisions', 'Collisions_percentage'])

    if DEBUG: print('Size', '#Tokens', 'Collisions', 'Collisions %', sep=',')
    for c in combinations:
        TOKEN_SIZE, token_frequency = c[0], c[1]
        TOKENS_PER_BLOCK = math.floor(BLOCK_SIZE / TOKEN_SIZE)

        nof_blocks = math.ceil(token_frequency / TOKENS_PER_BLOCK)
        block = (starting_token).to_bytes(BLOCK_SIZE, byteorder="big")

        tot_mem_create_time -= time.time(); nof_tokens += token_frequency; tot_mem_size += nof_blocks * 64
        memory = bytearray(block*nof_blocks)
        tot_mem_create_time += time.time()

        cipher = AES.new(master_key, AES.MODE_CBC, IV=salt)

        tot_enc_time -= time.time()
        encrypted_memory = cipher.encrypt(memory)
        tot_enc_time += time.time()

        tot_token_extraction_time -= time.time()
        collisions = 0
        tokens = set()
        for i in range(token_frequency):
            curr_token = int.from_bytes(encrypted_memory[i * TOKEN_SIZE:(i + 1) * TOKEN_SIZE], "big") >>1
            if curr_token in tokens:
                collisions += 1
            else:
                tokens.add(curr_token)
            tot_token_extraction_time += time.time()

        collision_percentage = collisions*100/token_frequency
        logfile_writer.writerow([TOKEN_SIZE, token_frequency, collisions, collision_percentage])
        if DEBUG:
            print(TOKEN_SIZE,
                  "{:.0e}".format(token_frequency),
                  collisions,
                  collision_percentage,
                  sep=',')

end_time = time.time()


"""TEST RESULTS"""
if inspect_output:
    # retrieve cpu model
    cpu = subprocess.check_output("lscpu", shell=True).strip().decode()
    cpu = re.search("Model name:(.*)\n", cpu)

    # time and memory
    tot_test_time = end_time - start_time
    tot_enc_time_percentage = 100 * tot_enc_time / tot_test_time
    tot_mem_create_time_percentage = 100 * tot_mem_create_time / tot_test_time
    tot_token_extraction_time_percentage = 100 * tot_token_extraction_time / tot_test_time
    tot_mem_size = (nof_blocks * 64) / 2 ** 23

    # performace
    throughputh = int(nof_tokens / (end_time - start_time))
    cycle_time = 1 / throughputh

    # formatting
    tot_test_time = "{:.3e}".format(tot_test_time)
    tot_enc_time = "{:.3e}".format(tot_enc_time)
    tot_enc_time_percentage = "{:.2f}".format(tot_enc_time_percentage)
    tot_mem_create_time = "{:.3e}".format(tot_mem_create_time)
    tot_mem_create_time_percentage = "{:.2f}".format(tot_mem_create_time_percentage)
    tot_token_extraction_time = "{:.3e}".format(tot_token_extraction_time)
    tot_token_extraction_time_percentage = "{:.2f}".format(tot_token_extraction_time_percentage)

    throughputh = "{:.3e}".format(throughputh)
    cycle_time = "{:.3e}".format(cycle_time)
    token_frequency = "{:.3e}".format(token_frequency)
    tot_mem_size = "{:.3e}".format(tot_mem_size)

    # printing
    if cpu:
        cpu = cpu.group().replace("Model name:", "").lstrip(" ")
        print(f"CPU Model: \t\t\t\t{cpu}", end="")
        print(f"Total test time:\t\t\t{tot_test_time}\t[s]")
    print(f"Total memory creation time:\t\t{tot_mem_create_time}\t[s] ({tot_mem_create_time_percentage}%)")
    print(f"Total memory encryption time:\t\t{tot_enc_time}\t[s] ({tot_enc_time_percentage}%)")
    print(f"Token extraction time (bytes to int):\t{tot_token_extraction_time}\t[s] ({tot_token_extraction_time_percentage}%)")
    print(f"Nof tokens produced:\t\t\t{token_frequency}\t[token]")
    print(f"Throughput:\t\t\t\t{throughputh}\t[token/s]")
    print(f"Cycle time:\t\t\t\t{cycle_time}\t[s/token]")
    print(f"Token memory:\t\t\t\t{tot_mem_size}\t[MB]")
