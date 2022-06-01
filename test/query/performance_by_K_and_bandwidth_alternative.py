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


MARKERS = ["o", "v", "^", "D", "s", "p", "h", "8", "*"]
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
    for bandwidth, curve in zip(sorted(by_bandwidth.keys()), curves):
        baseline_avg, baseline_var = baseline_time[bandwidth]
        worsening_curve = [(k, avg / baseline_avg, var) for k, avg, var in curve]
        worsening_curves.append(worsening_curve)
    return worsening_curves


def plot(curves, path, baseline_curve=None, worsening=False):
    if baseline_curve:
        # Baseline
        ks, avgs = zip(*baseline_curve)
        baseline = plt.plot(ks, avgs, color="black", linestyle="dashed")

    # Other lines
    lines = []
    for i, curve in enumerate(curves):
        ks, avgs, variances = zip(*curve)
        line = plt.plot(ks, avgs, marker=MARKERS[i], markersize=8)
        lines.extend(line)

    plt.xlabel('Size of the buckets')
    plt.ylabel(YLABELS[worsening])
    current_lines = baseline + lines if baseline_curve else lines
    current_labels = ["baseline"] + labels if baseline_curve else labels
    legend = plt.legend(current_lines, current_labels,
                        bbox_to_anchor=(-0.08, 1.02),
                        frameon=False,
                        loc='lower left',
                        prop={'size': 12},
                        ncol=4)
    for handle in legend.legendHandles:
        handle._legmarker.set_markersize(8)
    plt.ylim(bottom=0)
    plt.xticks([10, 25, 50, 75, 100])

    fig = plt.gcf()
    fig.savefig(os.path.join(directory, path),
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')
    if is_interactive: plt.show()
    # Clear the current Figure’s state without closing it
    plt.clf()


parser = argparse.ArgumentParser(
    description='Visualize performance evaluations by bandwidth.'
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

# Read multiple baseline (one per bandwidth configuration)
baseline_time = {}
for filename in os.listdir(directory):
    match = re.match(r"baseline-bandwidth=(\d+)Mbit-latency=\d+ms.csv",
                     filename)
    if match:
        bandwidth = int(match.group(1))
        baseline = os.path.join(directory, filename)
        print(f"[*] Read baseline: {baseline}\n")
        df_baseline = pd.read_csv(baseline, index_col="index")
        baseline_time[bandwidth] = compute_statistics([df_baseline["plain"]])

# baseline_curve = []
# for bandwidth in sorted(baseline_by_bandwidth.keys()):
#     df_baseline = baseline_by_bandwidth[bandwidth]
#     avg, _ = compute_statistics([df_baseline["plain"]])
#     baseline_curve.append((bandwidth, avg))

# Group performance evaluation files by bandwidth
by_bandwidth = defaultdict(list)
for filename in os.listdir(directory):
    match = re.search(r"-bandwidth=(\d+)Mbit-", filename)
    if match:
        bandwidth = int(match.group(1))
        by_bandwidth[bandwidth].append(filename)

print(by_bandwidth)

server_curves, total_curves = [], []
labels = []
for bandwidth in sorted(by_bandwidth.keys()):
    filenames = by_bandwidth[bandwidth]

    server_time, total_time = {}, {}

    # Group performance evaluation files by K
    by_K = defaultdict(list)
    for filename in filenames:
        match = re.search(r"-K=(\d+)-", filename)
        if match:
            K = int(match.group(1))
            by_K[K].append(filename)

    print(by_K)

    print(f"[*] bandwidth={bandwidth}Mbps")
    for K, filenames in by_K.items():
        print(f"[*] K={K}")
        current_server_time, current_total_time = [], []
        for filename in filenames:
            evaluation = os.path.join(directory, filename)
            print(f"Read {evaluation}")
            df = pd.read_csv(evaluation, index_col="index")

            partials = ["rewriting", "server", "decryption", "creation", "filtering"]
            df["wrapped"] = df[partials].sum(axis=1)

            current_server_time.append(df["server"])
            current_total_time.append(df["wrapped"])

        server_time[K] = compute_statistics(current_server_time)
        total_time[K] = compute_statistics(current_total_time)

    server_curves.append([(k, *server_time[k]) for k in sorted(server_time.keys())])
    total_curves.append([(k, *total_time[k]) for k in sorted(total_time.keys())])
    labels.append(f"{bandwidth}Mbit")

server_worsening_curves = compute_worsening_curves(server_curves)
total_worsening_curves = compute_worsening_curves(total_curves)

print("[*] Visualize performance overhead")
plot(server_curves, "server-time-by-K-and-bandwidth.pdf")
plot(total_curves, "overall-time-by-K-and-bandwidth.pdf")
baseline_worsening_curve = [(k, 1) for k in sorted(server_time.keys())]
plot(server_worsening_curves, "server-overhead-by-K-and-bandwidth.pdf", baseline_curve=baseline_worsening_curve, worsening=True)
plot(total_worsening_curves, "overall-overhead-by-K-and-bandwidth.pdf", baseline_curve=baseline_worsening_curve, worsening=True)
