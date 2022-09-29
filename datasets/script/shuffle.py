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
import random

import pandas as pd


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Shuffle tuples and group-id values to ensure ordering ' +
                    'does not leak information about the flattening process, ' +
                    'nor the original dataset'
    )
    parser.add_argument('dataset',
                        help='path of the dataset to shuffle')
    args = parser.parse_args()
    path = args.dataset

    df = pd.read_csv(path, index_col="INDEX")
    # Shuffle tuples of the dataset
    df = df.sample(frac=1)
    # Shuffle group-ids of the dataset respecting groups
    number_of_groups = df["GID"].nunique()
    random_gids = list(range(number_of_groups))
    random.shuffle(random_gids)
    df["GID"] = df["GID"].apply(lambda gid: random_gids[gid])
    df.to_csv(path, index_label="INDEX")
