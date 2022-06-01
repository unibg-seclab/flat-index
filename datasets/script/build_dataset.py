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
import glob

import pandas as pd


COLUMNS = ['ST', 'AGEP', "OCCP", "WAGP"]
NEW_COLUMNS = ["STATEFIP", "AGE", "OCC", "INCTOT"]
RENAME_COLUMNS = {before: after for before, after in zip(COLUMNS, NEW_COLUMNS)}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Build dataset starting from its split representation.'
    )
    parser.add_argument('input',
                        help='path containing the fragments of the dataset')
    parser.add_argument('output',
                        help='path where to store the dataset')
    args = parser.parse_args()
    directory = args.input
    path = args.output
    is_usa2018 = (os.path.basename(path) == "usa2018.csv")

    dataframes = []
    for basename in sorted(glob.glob(os.path.join(directory, "*.csv"))):
        df = pd.read_csv(basename, usecols=COLUMNS)
        dataframes.append(df)
    df = pd.concat(dataframes)

    df.fillna(0, inplace=True, downcast="infer")

    if is_usa2018:
        # Filter income values by their frequency
        counts = df["WAGP"].value_counts(sort=True)
        values_to_keep = counts.index[1:602]
        df = df[df["WAGP"].isin(values_to_keep)]
        # Extract a sample of 500000 tuples
        df = df.sample(500000)
        df.rename(columns=RENAME_COLUMNS, inplace=True)

    df["INDEX"] = [i for i in range(len(df.index))]
    df.to_csv(path,
              columns=["INDEX"] + (NEW_COLUMNS if is_usa2018 else COLUMNS),
              index=False)
