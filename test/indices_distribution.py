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

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import os
import pandas as pd


def bar(ax, series, title=None, xlabel=None):
    ax.bar(series.index, series, width=0.8)
    ax.axes.xaxis.set_visible(True)    
    ax.title.set_text(title)
    ax.set_xlabel(xlabel)
    ax.set_ylabel('Frequency')
#    ax.grid(axis='y', alpha=0.75)
    ax.ticklabel_format(axis='y', style='sci', scilimits=(0,0))

"""
EXAMPLE OF USAGE
./indices_distribution.py -k 25 ../datasets/usa2018/usa2018.csv ../datasets/usa2018/usa2018_25.csv ../datasets/wrapped/usa2018.csv --cols STATEFIP,AGE,OCC,INCTOT
"""
    
parser = argparse.ArgumentParser(description='Display frequency distributions of column indices.')
parser.add_argument('dataset', metavar='PLAIN', help='path to the plain dataset')
parser.add_argument('anonymized', metavar='ANONYMIZED', help='path to the anonymized dataset')
parser.add_argument('flattened', metavar='FLATTENED', help='path to the flattened dataset')
parser.add_argument('-k', metavar='K', help='number of tuples in the block')
parser.add_argument('--cols', metavar='COLS', help='comma-separated list of columns to be used')

args = parser.parse_args()

dataset = args.dataset
anonymized = args.anonymized
flattened = args.flattened
k = int(args.k) if args.k else 1
columns = args.cols.split(",") if args.cols else ["AGEP", "ST", "OCCP", "WAGP"]

test = os.path.realpath(os.path.dirname(__file__))
results = os.path.join(test, "results", "indices-distribution")

print("[*] Read plain dataset")
df = pd.read_csv(dataset)

print("[*] Read anonymized dataset")
adf = pd.read_csv(anonymized)

print("[*] Read flattened dataset")
fdf = pd.read_csv(flattened)

for i in range(len(columns)):

    fig, axs = plt.subplots(1, 3, figsize=(7,2.2))
    
    column = columns[i]

    counts = df[column].value_counts(sort=False)
    counts.sort_values(ascending=False, inplace=True, ignore_index=True)
    bar(axs[0], counts, title=f"Plaintext", xlabel="Values")

    print(f"{column} plain subplot printed")

    counts = adf[column].value_counts()/k
    counts.sort_values(ascending=False, inplace=True, ignore_index=True)
    bar(axs[1], counts, title=f"Indexed", xlabel="Generalizations")

    print(f"{column} indexed subplot printed")

    # Ensure the following rectangle matches the actual frequency distribution
    assert len(fdf.index) == fdf[column].nunique()

    nof_tokens = len(df[column])

    rectangle = patches.Rectangle((0,0), nof_tokens, 1, fill=True, color="cornflowerblue")
    axs[2].add_patch(rectangle)
    axs[2].title.set_text(f"Flattened")
    axs[2].set_ylabel('Frequency')        
    axs[2].set_xlabel("Tokens")    
    axs[2].axes.xaxis.set_visible(True)
    axs[2].set_yticks([0,1])
    axs[2].set_yticklabels(["0", "1"])
    axs[2].set_ylim([0,1.05])
    axs[2].set_xlim([-nof_tokens*0.05, nof_tokens*1.05])
    axs[2].ticklabel_format(axis='x', style='sci', scilimits=(0,0))

    print(f"{column} flattened subplot printed")    


    fig.suptitle(f"{column}", y=0.9)
    
    fig.tight_layout()
    plt.savefig(os.path.join(results, column + ".pdf"))
