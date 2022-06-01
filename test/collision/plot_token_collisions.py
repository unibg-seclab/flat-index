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


import csv
import math
import matplotlib.pyplot as plt
import os


""" USAGE
./plot_token_collisions.py; xdg-open ../results/collision/simulation_plot.pdf
"""

# configuration
test = os.path.realpath(os.path.join(__file__, "..", ".."))
results = os.path.join(test, "results", "collision")
log_path = os.path.join(results, "simulation.csv")
plot_curves = {}
frequencies = set()

# read the data from backup files
with open(log_path) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    line_count = 0
    for index, row in enumerate(csv_reader):
        if index == 0:
            continue
        token_size = int(row[0])
        token_freq = float(row[1])
        token_collisions = float(row[2])
        collision_ratio = token_collisions / token_freq
        if not token_size in plot_curves:
            plot_curves[token_size] = []
        plot_curves[token_size].append(collision_ratio)
        frequencies.add(token_freq)

# sort the experimental frequencies
frequencies = list(frequencies)
frequencies.sort()

# plot params
plt.rcParams["figure.figsize"] = (10,5)
marker_list = ["o", "^", "s", "p", "*"]

# compute and plot the B-day paradox cumulative probability
fs = [1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,200,
      300,400,500,600,700,800,900,1000,2000,3000,4000,5000,
      6000,7000,8000,9000,10000,20000,30000,40000,50000,60000,
      70000,80000,90000,100000,200000,300000,400000,500000,
      600000,700000,800000,900000,1000000,2000000,3000000,4000000,
      5000000,6000000,7000000,8000000,9000000,10000000,20000000,30000000,40000000,
      50000000,60000000,70000000,80000000,90000000]
simulated = {}
token_sizes = [8,16,24,32]
for ts in token_sizes:
    simulated[ts] = []
    for j in range(len(fs)):
        pigeonholes = 2*(2**(ts-1))
        exponent = -fs[j]/pigeonholes
        val = 1-math.exp(exponent)
        simulated[ts].append(val)
    plt.plot(fs, simulated[ts], linewidth = 1, marker=marker_list[4],
             linestyle=":",color="black", label =
             f'{math.ceil(float(ts)/8)} Bytes - B-Day Paradox')        

# plot the experimental curves
plt.xscale('log')
for i, key in enumerate(plot_curves.keys()):
    plt.plot(frequencies, plot_curves[key], linewidth = 1,
             linestyle=":", marker=marker_list[i], label = f'{key} Bytes - Experimental')

# plot refinemets
legend = plt.legend(framealpha=0.0)
plt.xlabel("Token frequency")
plt.ylabel("Cumulative probability")
#plt.title("Cumulative token collision probability given token size")
plt.savefig(os.path.join(results, "simulation_plot.pdf"))
