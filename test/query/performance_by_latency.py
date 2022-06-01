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
    "Performance ratio"
]

# Force pandas to show all columns
pd.set_option("display.max_columns", None)


def compute_statistics(samples):
    df = pd.DataFrame(zip(*samples))
    avg = df.mean(axis=1)
    variance = df.var(axis=1) / len(samples)
    # Compute a single avg and variance assuming they are iid
    return avg.mean(), variance.mean()


def compute_worsening_curves(curves):
    worsening_curves = []
    for curve in curves:
        worsening_curve = []
        for i in range(len(baseline_curve)):
            baseline_k, baseline_avg, baseline_var = baseline_curve[i]
            k, avg, var = curve[i]
            assert baseline_k == k
            worsening_curve.append((k, avg / baseline_avg, var))
        worsening_curves.append(worsening_curve)
    return worsening_curves


def plot(baseline_curve, curves, path, worsening=False):
    ks, avgs, variances = zip(*baseline_curve)

    # Baseline
    baseline = plt.plot(ks, avgs, linestyle="dashed", color="black")
    # Other lines
    lines = []
    for i, curve in enumerate(curves):
        ks, avgs, variances = zip(*curve)
        line = plt.plot(ks, avgs, marker=MARKERS[i], markersize=8)
        lines.extend(line)

    plt.xlabel('Latency (ms)')
    plt.ylabel(YLABELS[worsening])
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
    plt.ylim(bottom=0)
    plt.xticks([25, 50, 75, 100])

    fig = plt.gcf()
    fig.savefig(os.path.join(directory, path),
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')
    if is_interactive: plt.show()
    # Clear the current Figure’s state without closing it
    plt.clf()


parser = argparse.ArgumentParser(
    description='Visualize performance evaluations by latency.'
)
parser.add_argument('directory', metavar='PATH',
                    help='path to the directory containing performance data')
parser.add_argument('-i',
                    '--interactive',
                    action='store_true',
                    help='show plots')
args = parser.parse_args()
directory = os.path.realpath(args.directory)
is_interactive = args.interactive

# Read multiple baseline (one per latency configuration)
baseline_time = {}
for filename in os.listdir(directory):
    match = re.match(r"baseline-bandwidth=\d+Mbit-latency=(\d+)ms.csv",
                     filename)
    if match:
        latency = int(match.group(1))
        baseline = os.path.join(directory, filename)
        print(f"[*] Read baseline: {baseline}\n")
        df_baseline = pd.read_csv(baseline, index_col="index")
        baseline_time[latency] = compute_statistics([df_baseline["plain"]])

baseline_curve = [(latency / 2, *baseline_time[latency])
                  for latency in sorted(baseline_time.keys())]

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

server_curves, total_curves = [], []
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

    # Group performance evaluation files by latency
    by_latency = defaultdict(list)
    for filename in filenames:
        match = re.search(r"-latency=(\d+)ms-", filename)
        if match:
            latency = int(match.group(1))
            by_latency[latency].append(filename)

    print(f"[*] CONFIG: {config}")
    for latency, filenames in by_latency.items():
        print(f"[*] latency={latency}ms")
        current_server_time, current_total_time = [], []
        for filename in filenames:
            evaluation = os.path.join(directory, filename)
            print(f"Read {evaluation}")
            df = pd.read_csv(evaluation, index_col="index")

            partials = ["rewriting", "server", "decryption", "creation", "filtering"]
            df["wrapped"] = df[partials].sum(axis=1)

            current_server_time.append(df["server"])
            current_total_time.append(df["wrapped"])

        server_time[latency] = compute_statistics(current_server_time)
        total_time[latency] = compute_statistics(current_total_time)

    server_curves.append([(k / 2, *server_time[k]) for k in sorted(server_time.keys())])
    total_curves.append([(k / 2, *total_time[k]) for k in sorted(total_time.keys())])
    labels.append(config)

server_worsening_curves = compute_worsening_curves(server_curves)
total_worsening_curves = compute_worsening_curves(total_curves)

# Transform labels to be way more readable
labels = [RENAME[labels[i]] for i in range(len(labels))]

print("[*] Visualize performance overhead")
plot(baseline_curve, server_curves, "server-time-by-latency.pdf")
plot(baseline_curve, total_curves, "overall-time-by-latency.pdf")
baseline_worsening_curve = [(x / 2, 1, float("NaN")) for x in sorted(server_time.keys())]
plot(baseline_worsening_curve, server_worsening_curves, "server-overhead-by-latency.pdf", worsening=True)
plot(baseline_worsening_curve, total_worsening_curves, "overall-overhead-by-latency.pdf", worsening=True)
