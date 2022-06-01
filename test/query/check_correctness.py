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
import os
import re
from collections import defaultdict

import pandas as pd


parser = argparse.ArgumentParser(
    description='Check correctness of the queries by verifying the size of ' +
                'the query results.'
)
parser.add_argument('directory', metavar='PATH',
                    help='path to the directory containing performance data')
args = parser.parse_args()
directory = os.path.realpath(args.directory)


# Pick a baseline among the ones available checking them while at it
print("[*] Baseline\n")
df_baseline = None
is_latency = False
for filename in os.listdir(directory):
    if filename.startswith("baseline"):
        baseline = os.path.join(directory, filename)
        if not df_baseline:
            print(f"Read {baseline}\n")
            df_baseline = pd.read_csv(baseline, index_col="index")
        else:
            print(f"Checking {baseline}...\n")
            df = pd.read_csv(baseline, index_col="index")
            df = df.join(df_baseline["nof_result_tuples"], rsuffix="_plain")
            is_correct = (df["nof_result_tuples"] == df["nof_result_tuples_plain"]).all()
            if not is_correct:
                print(f"ERROR: {baseline} returns a wrong number of tuples")
            is_latency = True

# Check that every performance evaluation match the size of the plain result
print("[*] Checking plain against every performance evalutation\n")
for filename in os.listdir(directory):
    if filename != "baseline.csv":
        wrapped = os.path.join(directory, filename)
        print(f"Checking {wrapped}...\n")
        df = pd.read_csv(wrapped, index_col="index")
        df = df.join(df_baseline["nof_result_tuples"], rsuffix="_plain")
        is_correct = (df["nof_result_tuples"] == df["nof_result_tuples_plain"]).all()
        if not is_correct:
            print(f"ERROR: {wrapped} returns a wrong number of tuples")

# Group performance evaluation files by configuration
by_config = defaultdict(list)
for filename in os.listdir(directory):
    match = re.search(r"=\d+(ms)?-(.*)\.csv", filename)
    if match:
        config = match.group(2)
        by_config[config].append(filename)

for config, filenames in by_config.items():
    # Skip no flatten representations
    if config in ["no-gid", "no-gid-indices", "no-gid-kv-indices"]:
        continue

    # Skip split relational representation
    if config in ["no-gid-indices", "runtime-indices"]:
        continue

    # # Skip configurations with gid
    # if config in ["gid", "gid-kv"]:
    #     continue

    # Group performance evaluation files by K
    by_K = defaultdict(list)
    if not is_latency:
        for filename in filenames:
            match = re.search(r"-K=(\d+)-", filename)
            if match:
                K = int(match.group(1))
                by_K[K].append(filename)
    else:
        by_K["Unknown"] = [filename
                           for filename in filenames
                           if not filename.startswith("baseline")]

    # Check that every performance evaluation belonging to a group has the same
    # number of the enctuples and plaintext tuples
    print(f"[*] Checking performance evaluations of the {config} config")
    for K, filenames in by_K.items():
        print(f"[*] K={K}")
        df_wrapped = None
        for filename in filenames:
            wrapped = os.path.join(directory, filename)
            if df_wrapped is None:
                print(f"Read {wrapped}\n")
                df_wrapped = pd.read_csv(wrapped, index_col="index")
            else:
                print(f"Checking {wrapped}...\n")
                df = pd.read_csv(wrapped, index_col="index")
                df = df.join(df_wrapped, rsuffix="_wrapped")
                is_same_enctuples = (df["nof_enctuples"] == df["nof_enctuples_wrapped"]).all()
                is_same_tuples = (df["nof_plaintext_tuples"] == df["nof_plaintext_tuples_wrapped"]).all()
                is_correct = is_same_enctuples and is_same_tuples
                if not is_correct:
                    print(f"ERROR: {wrapped} returns a wrong number of tuples")
