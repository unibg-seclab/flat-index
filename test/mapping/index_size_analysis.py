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
import multiprocessing
import os
import pandas as pd
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psutil

import functools


def perc_to_bpt(x, dataset_size, portion):
    return x *  dataset_size / portion

def bpt_to_perc(x, dataset_size, portion):
    return x * portion / dataset_size

def plot_results(of_gid=False):
    fig, ax = plt.subplots()
    plt.gcf().subplots_adjust(bottom=0.15, left=0.15, right=0.77)
    fig2, ax2 = plt.subplots()
    plt.gcf().subplots_adjust(bottom=0.15, left=0.15, right=0.77)
    fname = f"{mapping_results}/Index_size_scaling.csv" if not of_gid else f"{mapping_results}/Index_size_scaling_GID.csv"
    data = pd.read_csv(fname)
    data = data[data.column == "all"]
    f = 15
    data_size = len(data.index)
    cols = 0
    for k,color,m in zip([100, 75, 50, 25, 10],["#7C5200", "#C4DF10", "m", "r", "cyan"], ["o", "*", "^", "D", "s"]):
        cols+=1
        ax.plot([x / 10 for x in range(5,35,5)], [x for x in data[data["k"] == k]["percentage"]], marker=m,label=f"k={k}", linewidth=2.5, markersize=8.0)
        #ax.legend(fontsize=f, loc="upper right", ncol=cols, prop={'size': 10})
        ax.set_xlabel("Millions of tuples", fontsize=f)
        ax.set_ylabel("Map size (% of plaintext)", fontsize=f)
        ax.set_xticklabels([x for x in ax.get_xticks()], fontsize=f)
        ax.set_yticklabels(["%.0f" % x for x in ax.get_yticks()], fontsize=f)
        #fig.suptitle("Index % vs Number of Tuples in the Dataset", fontsize=f)
        ax2.plot([x / 10 for x in range(5,35,5)], [x / y for x,y in zip(data[data["k"] == k]["map_size"], data[data["k"] == k]["portion"])], color=f'{color}', marker=m, label=f"K={k}", linewidth=2.5, markersize=8.0)
        #ax2.legend(fontsize=f, loc="upper right", ncol=cols, prop={'size': 10})
        ax2.set_xlabel("Millions of tuples", fontsize=f)
        ax2.set_ylabel("Bytes per tuple", fontsize=f)
        ax2.set_xticklabels([x for x in ax2.get_xticks()], fontsize=f)
        ax2.set_yticklabels(["%.2f" % x for x in ax2.get_yticks()], fontsize=f)
        #fig2.suptitle("Bytes per tuple vs Number of Tuples in the Dataset", fontsize=f)
    
    handles, labels = ax.get_legend_handles_labels()
    order_map = { f"k={k}": x for k,x in zip([10, 25, 50, 75, 100], [1, 2, 3, 4, 5])}
    entries = [x for x in zip(handles, labels)]
    entries.sort(key=lambda x: order_map[x[1]])
    legend = fig.legend([x[0] for x in entries], [x[1] for x in entries], frameon=False,
                          loc='lower left',
                          mode="expand",
                          bbox_to_anchor=(0.025, 0.93, 0.9, 0.2),
                          prop={'size': 12},
                          ncol=cols)

    legend2 = fig2.legend(frameon=False,
                          loc='upper center',
                          prop={'size': 12},
                          ncol=cols)

    #lin_space1 = np.linspace(0.0, 0.06, num=25)[0::4] if not of_gid else np.linspace(0.0, 0.09, num=25)[0::4]
    #lin_space2 = np.linspace(0.0, 1.20, num=15)[0::2] if not of_gid else np.linspace(0.0, 1.90, num=15)[0::2]
    #ylim1, ylim2 = (0.062, 1.20) if not of_gid else (0.092, 1.90)
    lin_space1 = np.linspace(0.0, 0.09, num=10)
    lin_space2 = np.linspace(0.0, 1.90, num=20)[0::2]
    ylim1, ylim2 = (0.092, 1.90)

    ax2.set_yticks(lin_space2)
    ax2.set_ylim(ax2.get_ylim()[0], ylim2)
    ax2.set_yticklabels(["%.2f" % x for x in ax2.get_yticks()], fontsize=f)
    ax.set_yticks(lin_space1)
    ax.set_ylim(ax.get_ylim()[0], ylim1)
    #ax.set_yticklabels(["%.2f" % (x*100) for x in ax.get_yticks()], fontsize=f)
    ax.set_yticklabels([f"{int(x*100)}" for x in ax.get_yticks()], fontsize=f)
    bpt_name = f"{mapping_results}/Plot_Index_BPT.pdf" if not of_gid else f"{mapping_results}/Plot_Index_BPT_GID.pdf" 
    perc_name = f"{mapping_results}/Plot_Index_Percentage.pdf" if not of_gid else f"{mapping_results}/Plot_Index_Percentage_GID.pdf"
    fig.savefig(perc_name,
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')

    fig2.savefig(bpt_name,
                 bbox_extra_artists=(legend2, ),
                 bbox_inches='tight')


    from_perc = functools.partial(perc_to_bpt, dataset_size=data[data["k"] == 25]["dataset_size"].iloc[0], portion=data[data["k"] == 25]["portion"].iloc[0])
    from_bpt = functools.partial(bpt_to_perc, dataset_size=data[data["k"] == 25]["dataset_size"].iloc[0], portion=data[data["k"] == 25]["portion"].iloc[0])
    sec = ax.secondary_yaxis('right', functions=(from_perc, from_bpt))
    sec.set_ylabel("Bytes per tuple", fontsize=f)
    size_name  = f"{mapping_results}/Plot_Index_Sizes.pdf" if not of_gid else f"{mapping_results}/Plot_Index_Sizes_GID.pdf"
    fig.savefig(size_name,
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')
    sec.set_yticks(lin_space2)
    sec.set_yticklabels(["%.1f" % x for x in sec.get_yticks()], fontsize=f)
    #fig.set_size_inches(8, 3.5)
    fig.tight_layout(rect=[0,0,0.925,0.975])
    fig.savefig(size_name,
                bbox_extra_artists=(legend, ),
                bbox_inches='tight')

def store_results(results, map_size, size, k, column, submodule):
    results["portion"].append(size)
    results["column"].append(column)
    results["k"].append(k)
    results["dataset_size"].append(os.path.getsize(f"{submodule}/dataset/usa2019.csv"))
    results["map_size"].append(map_size)
    results["percentage"].append(map_size / os.path.getsize(f"{submodule}/dataset/usa2019.csv"))
    return results


def on_disk_test():
    index_size_results = Path(f"{mapping_results}/to_map")
    index_size_results.mkdir(parents=True, exist_ok=True)

    os.system(f"rm -f {mapping_results}/to_map/*")
    results = {"portion": [], "column": [],"map_size": [], "k": [], "dataset_size": [], "percentage": []}
    results_GID = {"portion": [], "column": [],"map_size": [], "k": [], "dataset_size": [], "percentage": []}

    for k in [10, 25, 50, 75, 100]:

        portions = [x * 100000 for x in range(5, 35, 5)]
        for size in portions:
            df = pd.read_csv(os.path.join(datasets, "usa2019.csv"))
            if size != 'all':
                df = df.sample(n=size)

            df.to_csv(f'{submodule}/dataset/usa2019.csv', index=False)
            os.system(f'make -C {submodule} clean')
            os.system(f'make -C {submodule} usa2019 K={k} CONFIG=usa2019 WORKERS={jobs}')
            os.system(f"cp {submodule}/dataset/usa2019.csv {mapping_results}/to_map/usa2019.csv")
            os.system(f"cp {submodule}/anonymized/usa2019.csv {mapping_results}/to_map/usa2019_{k}.csv")
            for c in ["ST", "OCCP"]:
                os.system(f"make -C {root} test_categorical_mapping DATASETS={mapping_results}/to_map CATEGORICAL={c} PORTION={size}")
                os.system(f"make -C {root} test_categorical_mapping_gid DATASETS={mapping_results}/to_map CATEGORICAL={c} PORTION={size}")
            for c in ["AGEP", "WAGP"]:
                os.system(f"make -C {root} test_mapping DATASETS={mapping_results}/to_map COLUMN={c} PORTION={size}")
                os.system(f"make -C {root} test_mapping_gid DATASETS={mapping_results}/to_map COLUMN={c} PORTION={size}")
            for token_type in ["", "GID"]:
                # check on disk size
                map_size = 0
                for c in ["ST", "OCCP"]:
                    sizes = []
                    for map_type in ["bitmap", "roaring", "set"]:
                        fname = f"{mapping_results}/usa2019_{k}_{c}_{map_type}.pkl" if token_type == "" else f"{mapping_results}/usa2019_{k}_{c}_{map_type}_GID.pkl"
                        sizes.append(os.path.getsize(fname))
                    map_size += min(sizes)
                    if token_type == "":
                        results = store_results(results, min(sizes), size, k, c, submodule)
                    else:
                        results_GID = store_results(results_GID, min(sizes), size, k, c, submodule)
                for c in ["AGEP", "WAGP"]:
                    sizes = []
                    for map_type in ["", "interval-tree"]:
                        if token_type == "":
                            map_path = f"{mapping_results}/usa2019_{k}_{c}_{map_type}.pkl" if map_type != "" else f"{mapping_results}/usa2019_{k}_{c}.pkl"
                        else:
                            map_path = f"{mapping_results}/usa2019_{k}_{c}_{map_type}_GID.pkl" if map_type != "" else f"{mapping_results}/usa2019_{k}_{c}_GID.pkl"
                        sizes.append(os.path.getsize(map_path))
                    map_size += min(sizes)
                    if token_type == "":
                        results = store_results(results, min(sizes), size, k, c, submodule)
                    else:
                        results_GID = store_results(results_GID, map_size, size, k, c, submodule)
                os.system(f"rm {mapping_results}/to_map/usa2019_{k}.csv {mapping_results}/to_map/usa2019.csv")
                #os.system(f"rm {mapping_results}/usa2019_*")
                if token_type == "":
                    results = store_results(results, map_size, size, k, "all", submodule)
                else:
                    results_GID = store_results(results_GID, map_size, size, k, "all", submodule)
                rname = f"{mapping_results}/Index_size_scaling.csv" if token_type == "" else f"{mapping_results}/Index_size_scaling_GID.csv"
                if token_type == "":
                    pd.DataFrame(data=results).to_csv(rname,index=False)
                else:
                    pd.DataFrame(data=results_GID).to_csv(rname,index=False)



if __name__ == '__main__':
    # Identify cpu count limit according to memory consumption of each worker
    memory_info = psutil.virtual_memory()
    total_memory_gigs = (memory_info.total / 1024**3)
    cpu_count_limit = min(multiprocessing.cpu_count(), int(total_memory_gigs // 2))

    parser = argparse.ArgumentParser(
        description='Retrieve the size of the mappings with varying K and ' +
                    'number of tuples.'
    )
    parser.add_argument('-j',
                        '--jobs',
                        type=int,
                        default=cpu_count_limit,
                        help='number of jobs available to perform the index creation')
    parser.add_argument('-p',
                        '--plot-only',
                        dest='plot_only',
                        action='store_true',
                        help='Only create the plots without calculating the maps')

    args = parser.parse_args()
    jobs = min(args.jobs, cpu_count_limit)
    plot_only = args.plot_only

    root = os.path.realpath(os.path.join(__file__, *([os.path.pardir] * 3)))
    datasets = os.path.join(root, "datasets", "usa2019")
    submodule = os.path.join(root, "submodules", "spark-mondrian",
                             "distributed")
    mapping_results = os.path.join(root, "test", "results", "mapping")
    if not plot_only:
        on_disk_test()
    plot_results()
    plot_results(True)
