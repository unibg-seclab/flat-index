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
from email.mime import base
import glob
import math
import os
import re
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


# Fix font size of the plots
plt.rcParams.update({'font.size': 15})


MARKERS = ["o", "v", "^", "D", "s", "p", "h", "8", "*"]
RENAME = {
    "no-gid": "PostgreSQL not flat",
    "gid": "PostgreSQL with group-ids",
    "runtime": "PostgreSQL with tokens",
    "no-gid-indices": "PostgreSQL not flat (secondary table)",
    "runtime-indices": "PostgreSQL with tokens (secondary table)",
    "gid-kv": "Redis with group-ids",
    "no-gid-kv-indices": "Redis not flat",
    "runtime-kv-indices": "Redis with tokens",
}
YLABELS = [
    "Time (s)",
    "Performance ratio",
    "#Tuples downloaded",
    "Ratio of the #Tuples downloaded"
]

# Force pandas to show all columns
pd.set_option("display.max_columns", None)


def compute_statistics_per_query(samples, column=None):
    df = pd.DataFrame(zip(*samples))
    avg = df.mean(axis=1)
    variance = df.var(axis=1) / len(samples)
    return pd.DataFrame(zip(avg, variance),
                        columns=[column, column + "_variance"])


def compute_worsening_curves(baseline_curve, curves):
    worsening_curves = []
    for curve in curves:
        worsening_curve = []
        for i in range(len(baseline_curve)):
            baseline_bin, baseline_avg, baseline_var = baseline_curve[i]
            bin, avg, var = curve[i]
            assert baseline_bin == bin
            worsening_curve.append((bin, avg / baseline_avg, var))
        worsening_curves.append(worsening_curve)
    return worsening_curves


def plot(baseline_curve, curves, path, size=False, worsening=False):
    ks, avgs, variances = zip(*baseline_curve)

    # Baseline
    baseline = plt.plot(list(map(str, ks)), avgs,
                        linestyle="dashed", color="black")
    # Other lines
    lines = []
    for i, curve in enumerate(curves):
        ks, avgs, variances = zip(*curve)
        line = plt.plot(list(map(str, ks)), avgs,
                        marker=MARKERS[i], markersize=8)
        lines.extend(line)

    plt.xlabel('Percentage of the dataset the query selects')
    plt.ylabel(YLABELS[2*size + worsening])
    legend = plt.legend(baseline + lines, ["baseline"] + labels,
                        bbox_to_anchor=(-0.08, 1),
                        frameon=False,
                        loc='lower left',
                        prop={'size': 12},
                        ncol=2)
    for handle in legend.legendHandles:
        handle._legmarker.set_markersize(8)
    plt.ylim(bottom=0)

    fig = plt.gcf()
    fig.savefig(os.path.join(directory, path),
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')
    if is_interactive: plt.show()
    # Clear the current Figure’s state without closing it
    plt.clf()


parser = argparse.ArgumentParser(
    description='Visualize performance evaluations by size.'
)
parser.add_argument('directory', metavar='PATH',
                    help='path to the directory containing performance data')
parser.add_argument('-i',
                    '--interactive',
                    action='store_true',
                    help='show plots')
parser.add_argument('-k',
                    metavar='K',
                    default=50,
                    type=int,
                    help='target bucket size (default: 50)')
parser.add_argument('-p',
                    '--performance',
                    action='store_true',
                    help='visualize performance evaluations')
parser.add_argument('-s',
                    '--size',
                    action='store_true',
                    help='visualize size of the query results')

args = parser.parse_args()
directory = os.path.realpath(args.directory)
is_interactive = args.interactive
k = args.k
is_performance = args.performance
is_size = args.size

# When no flags are given, visualize performance evaluations
if not is_performance and not is_size:
    is_performance = True

baseline = glob.glob(os.path.join(directory, f"baseline*.csv"))[0]
print(f"[*] Read baseline: {baseline}\n")
df_baseline = pd.read_csv(baseline, index_col="index")

# max_query_size = 0.1
# Keep most significant digit of max query size by rounding it up
max_query_size = max(df_baseline["size"])
power = -int(math.floor(math.log10(abs(max_query_size))))
max_query_size = math.ceil(max_query_size * 10**power) / 10**power

# Group baseline by size of the query results
df_baseline["size_bin"] = pd.cut(df_baseline["size"],
                                 bins=np.linspace(0, max_query_size, num=6))
baseline_by_size = df_baseline.groupby("size_bin")

baseline_time_curve = [(index, value, float("NaN"))
                       for index, value in baseline_by_size["plain"].mean().iteritems()]
baseline_size_curve = [(index, value, float("NaN"))
                       for index, value in baseline_by_size["nof_result_tuples"].mean().iteritems()]

# Filter performance evaluation files by K and group them by configuration
by_config = defaultdict(list)
for filename in os.listdir(directory):
    match = re.search(r"-K=" + str(k) + r"-.*-latency=\d+ms-(.*)\.csv",
                      filename)
    if match:
        config = match.group(1)
        by_config[config].append(filename)

server_curves, server_worsening_curves = [], []
total_curves, total_worsening_curves = [], []
size_curves, size_worsening_curves = [], []
labels = []
for config in sorted(by_config.keys(), reverse=True):
    filenames = by_config[config]

    # Skip no flatten representations
    if config in ["no-gid", "no-gid-indices", "no-gid-kv-indices"]:
        continue

    # Skip split relational representation
    if config in ["no-gid-indices", "runtime-indices"]:
        continue

    # # Skip configurations with gid
    # if config in ["gid", "gid-kv"]:
    #     continue

    print(f"[*] CONFIG: {config}")

    # Resolve repetitions of the same configuration
    current_query_size = []
    current_server_time, current_total_time, current_size = [], [], []
    for filename in filenames:
        evaluation = os.path.join(directory, filename)
        print(f"Read {evaluation}")
        df = pd.read_csv(evaluation, index_col="index")

        partials = ["rewriting", "server", "decryption", "creation", "filtering"]
        df["total"] = df[partials].sum(axis=1)

        current_query_size.append(df["size"])
        current_server_time.append(df["server"])
        current_total_time.append(df["total"])
        current_size.append(df["nof_plaintext_tuples"])

    query_size = compute_statistics_per_query(current_query_size, column="size")
    server_time = compute_statistics_per_query(current_server_time, column="server")
    total_time = compute_statistics_per_query(current_total_time, column="total")
    size = compute_statistics_per_query(current_size, column="nof_plaintext_tuples")
    df = pd.concat([query_size, server_time, total_time, size], axis=1)

    # Group by size of the query results
    df["size_bin"] = pd.cut(df["size"], bins=np.linspace(0, max_query_size, num=6))
    by_size = df.groupby("size_bin").mean()

    server_curves.append([(bin, row["server"], row["server_variance"])
                          for bin, row in by_size[["server", "server_variance"]].iterrows()])
    total_curves.append([(bin, row["total"], row["total_variance"])
                          for bin, row in by_size[["total", "total_variance"]].iterrows()])
    size_curves.append([(bin, row["nof_plaintext_tuples"], row["nof_plaintext_tuples_variance"])
                        for bin, row in by_size[["nof_plaintext_tuples", "nof_plaintext_tuples_variance"]].iterrows()])
    labels.append(config)

baseline_worsening_curve = [(bin, 1, float("NaN")) for bin, *_ in baseline_time_curve]
server_worsening_curves = compute_worsening_curves(baseline_time_curve, server_curves)
total_worsening_curves = compute_worsening_curves(baseline_time_curve, total_curves)
size_worsening_curves = compute_worsening_curves(baseline_size_curve, size_curves)

# Transform labels to be way more readable
labels = [RENAME[labels[i]] for i in range(len(labels))]

print("[*] Visualize performance overhead")
if is_performance:
    plot(baseline_time_curve, server_curves, "server-time-by-size.pdf")
    plot(baseline_time_curve, total_curves, "overall-time-by-size.pdf")
    plot(baseline_worsening_curve, server_worsening_curves, "server-overhead-by-size.pdf", worsening=True)
    plot(baseline_worsening_curve, total_worsening_curves, "overall-overhead-by-size.pdf", worsening=True)

if is_size:
    plot(baseline_size_curve, size_curves, "size-bytes-by-size.pdf", size=True)
    plot(baseline_worsening_curve, size_worsening_curves, "size-overhead-by-size.pdf", size=True, worsening=True)
