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
import glob
import os
import re
from collections import defaultdict

import matplotlib.pyplot as plt
import pandas as pd


# Fix font size of the plots
plt.rcParams.update({'font.size': 15})


MARKERS = ["o", "s", "^", "D", "*", "p", "h", "8", "v"]
RENAME = {
    "no-gid": "PostgreSQL not flat",
    # "gid": "PostgreSQL with group-ids",
    "gid": "Group-based, PostgreSQL",
    # "runtime": "PostgreSQL with tokens",
    "runtime": "Value-based, PostgreSQL",
    "no-gid-indices": "PostgreSQL not flat (secondary table)",
    "runtime-indices": "PostgreSQL with tokens (secondary table)",
    # "gid-kv": "Redis with group-ids",
    "gid-kv": "Group-based, Redis",
    "no-gid-kv-indices": "Redis not flat",
    # "runtime-kv-indices": "Redis with tokens",
    "runtime-kv-indices": "Value-based, Redis",
}
YLABELS = [
    "Time (s)",
    "Performance ratio",
    "#Tuples downloaded",
    "#t downloaded/#t in result"
]


def compute_statistics(samples):
    df = pd.DataFrame(zip(*samples))
    avg = df.mean(axis=1)
    variance = df.var(axis=1) / len(samples)
    # Compute a single avg and variance assuming they are iid
    return avg.mean(), variance.mean()


def compute_worsening_curves(baseline, curves):
    return [[(k, avg / baseline, var) for k, avg, var in curve]
            for curve in curves]


def plot(baseline, curves, path, size=False, worsening=False):
    ks, avgs, variances = zip(*curves[0])

    # Baseline
    baseline = plt.plot(ks, [baseline] * len(ks), linestyle="dashed", color="black")
    # Other lines
    lines = []
    for i, curve in enumerate(curves):
        ks, avgs, variances = zip(*curve)
        line = plt.plot(ks, avgs, marker=MARKERS[i], markersize=8)
        lines.extend(line)

    plt.xlabel('k')
    plt.ylabel(YLABELS[2*size + worsening])
    # Put baseline as the 3rd element in the legend
    current_lines = lines[:2] + baseline + lines[2:]
    current_labels = labels[:2] + ["baseline"] + labels[2:]
    legend = plt.legend(current_lines, current_labels,
                        bbox_to_anchor=(-0.1, 1),
                        frameon=False,
                        loc='lower left',
                        prop={'size': 12},
                        ncol=2)
    for handle in legend.legendHandles:
        handle._legmarker.set_markersize(8)
    plt.ylim(bottom=0)#, top=1.26)
    plt.xticks([10, 25, 50, 75, 100])

    fig = plt.gcf()
    fig.savefig(os.path.join(directory, path),
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')
    if is_interactive: plt.show()
    # Clear the current Figure’s state without closing it
    plt.clf()


parser = argparse.ArgumentParser(
    description='Visualize performance evaluations by K.'
)
parser.add_argument('directory', metavar='PATH',
                    help='path to the directory containing performance data')
parser.add_argument('-i',
                    '--interactive',
                    action='store_true',
                    help='show plots')
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
is_performance = args.performance
is_size = args.size

# When no flags are given, visualize performance evaluations
if not is_performance and not is_size:
    is_performance = True

baseline = glob.glob(os.path.join(directory, f"baseline*.csv"))[0]
print(f"[*] Read baseline: {baseline}\n")
df_baseline = pd.read_csv(baseline, index_col="index")
avg_baseline_time = df_baseline["plain"].mean()
avg_baseline_size = df_baseline["nof_result_tuples"].mean()

# Group performance evaluation files by configuration
by_config = defaultdict(list)
for filename in os.listdir(directory):
    match = re.search(r"-latency=\d+ms-(.*)\.csv", filename)
    if match:
        config = match.group(1)
        by_config[config].append(filename)

# Sort configs depending on their content
configs = sorted(by_config.keys(), reverse=True)
if configs == ['runtime-kv-indices', 'runtime', 'gid-kv', 'gid']:
    configs = ['runtime', 'runtime-kv-indices', 'gid', 'gid-kv']

server_curves, server_worsening_curves = [], []
total_curves, total_worsening_curves = [], []
size_curves, size_worsening_curves = [], []
labels = []
for config in configs:
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

    server_time, total_time = {}, {}
    server_worsening, total_worsening = {}, {}
    size, size_worsening = {}, {}

    # Group performance evaluation files by K
    by_K = defaultdict(list)
    for filename in filenames:
        match = re.search(r"-K=(\d+)-", filename)
        if match:
            K = int(match.group(1))
            by_K[K].append(filename)

    print(f"[*] CONFIG: {config}")
    for K, filenames in by_K.items():
        print(f"[*] K={K}")
        current_server_time, current_total_time, current_size = [], [], []
        for filename in filenames:
            evaluation = os.path.join(directory, filename)
            print(f"Read {evaluation}")
            df = pd.read_csv(evaluation, index_col="index")

            partials = ["rewriting", "server", "decryption", "creation", "filtering"]
            df["total"] = df[partials].sum(axis=1)

            current_server_time.append(df["server"])
            current_total_time.append(df["total"])
            current_size.append(df["nof_plaintext_tuples"])

        server_time[K] = compute_statistics(current_server_time)
        total_time[K] = compute_statistics(current_total_time)
        size[K] = compute_statistics(current_size)

    server_curves.append([(k, *server_time[k])
                          for k in sorted(server_time.keys())])
    total_curves.append([(k, *total_time[k])
                         for k in sorted(total_time.keys())])
    size_curves.append([(k, *size[k]) for k in sorted(size.keys())])
    labels.append(config)

server_worsening_curves = compute_worsening_curves(avg_baseline_time, server_curves)
total_worsening_curves = compute_worsening_curves(avg_baseline_time, total_curves)
size_worsening_curves = compute_worsening_curves(avg_baseline_size, size_curves)

# Transform labels to be way more readable
labels = [RENAME[labels[i]] for i in range(len(labels))]

print("[*] Visualize performance overhead")
if is_performance:
    plot(avg_baseline_time, server_curves, "server-time-by-K.pdf")
    plot(avg_baseline_time, total_curves, "overall-time-by-K.pdf")
    plot(1, server_worsening_curves, "server-overhead-by-K.pdf", worsening=True)
    plot(1, total_worsening_curves, "overall-overhead-by-K.pdf", worsening=True)

if is_size:
    plot(avg_baseline_size, size_curves, "size-bytes-by-K.pdf", size=True)
    plot(1, size_worsening_curves, "size-overhead-by-K.pdf", size=True, worsening=True)
