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
import matplotlib.ticker as mticker
import pandas as pd


# Fix font size of the plots
plt.rcParams.update({'font.size': 15})


MARKERS = ["o", "s", "^", "D", "*", "p", "h", "8", "v"]
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
    baseline = plt.plot(ks, avgs, color="black", linestyle="dashed")
    # Other lines
    lines = []
    for i, curve in enumerate(curves):
        ks, avgs, variances = zip(*curve)
        line = plt.plot(ks, avgs, marker=MARKERS[i], markersize=8)
        lines.extend(line)

    plt.xlabel('Bandwidth (Mbps)')
    plt.ylabel(YLABELS[worsening])
    # Put baseline as the 3rd element in the legend
    current_lines = lines[:1] + baseline + lines[1:]
    current_labels = labels[:1] + ["baseline"] + labels[1:]
    legend = plt.legend(current_lines, current_labels,
                        bbox_to_anchor=(-0.12, 1, 1.16, 1),
                        frameon=False,
                        loc='lower left',
                        mode="expand",
                        prop={'size': 12},
                        ncol=5)
    for handle in legend.legendHandles:
        handle._legmarker.set_markersize(8)
    plt.ylim(bottom=0)
    plt.xscale("log")
    plt.xticks([1, 10, 100, 1000])
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mticker.ScalarFormatter())

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

baseline_curve = [(k, *baseline_time[k]) for k in sorted(baseline_time.keys())]

# Group performance evaluation files by K
by_K = defaultdict(list)
for filename in os.listdir(directory):
    match = re.search(r"-K=(\d+)-", filename)
    if match:
        K = int(match.group(1))
        by_K[K].append(filename)

server_curves, total_curves = [], []
labels = []
for K in sorted(by_K.keys()):
    filenames = by_K[K]

    server_time, total_time = {}, {}

    # Group performance evaluation files by bandwidth
    by_bandwidth = defaultdict(list)
    for filename in filenames:
        match = re.search(r"-bandwidth=(\d+)Mbit-", filename)
        if match:
            bandwidth = int(match.group(1))
            by_bandwidth[bandwidth].append(filename)

    print(f"[*] K={K}")
    for bandwidth, filenames in by_bandwidth.items():
        print(f"[*] bandwidth={bandwidth}Mbps")
        current_server_time, current_total_time = [], []
        for filename in filenames:
            evaluation = os.path.join(directory, filename)
            print(f"Read {evaluation}")
            df = pd.read_csv(evaluation, index_col="index")

            partials = ["rewriting", "server", "decryption", "creation", "filtering"]
            df["wrapped"] = df[partials].sum(axis=1)

            current_server_time.append(df["server"])
            current_total_time.append(df["wrapped"])

        server_time[bandwidth] = compute_statistics(current_server_time)
        total_time[bandwidth] = compute_statistics(current_total_time)

    server_curves.append([(k, *server_time[k]) for k in sorted(server_time.keys())])
    total_curves.append([(k, *total_time[k]) for k in sorted(total_time.keys())])
    labels.append(f"k={K}")

server_worsening_curves = compute_worsening_curves(server_curves)
total_worsening_curves = compute_worsening_curves(total_curves)

print("[*] Visualize performance overhead")
plot(baseline_curve, server_curves, "server-time-by-K-and-bandwidth.pdf")
plot(baseline_curve, total_curves, "overall-time-by-K-and-bandwidth.pdf")
baseline_worsening_curve = [(k, 1, float("NaN")) for k in sorted(server_time.keys())]
plot(baseline_worsening_curve, server_worsening_curves, "server-overhead-by-K-and-bandwidth.pdf", worsening=True)
plot(baseline_worsening_curve, total_worsening_curves, "overall-overhead-by-K-and-bandwidth.pdf", worsening=True)
