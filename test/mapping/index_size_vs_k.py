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

import matplotlib.pyplot as plt
import pandas as pd

def bpt_to_abs(x):
    return x*3239553/1024
def abs_to_bpt(x):
    return x*1024/3239553

def plot_num():
    num_rows = 3239553
    fig, ax = plt.subplots()
    fig2, ax2 = plt.subplots()
    plt.gcf().subplots_adjust(bottom=0.15, left=0.20)
    data = pd.read_csv(os.path.join(results, "size_num_usa2019.csv"))
    f = 15
    data_size = len(data.index)
    legend = True
    mp = {10 : 25, 25:50, 50:75, 75:100, 100:125}
    for k,color,m in zip([100, 75, 50, 25, 10],["#7C5200", "#C4DF10", "m", "r", "cyan"], ["s", "o", "^", "D", "*"]):
        wagp_y = [x / 1024  for x in data[(data["k"] == k) & (data["column"]=="WAGP")]["map_size"]]
        agep_y = [x / 1024 for x in data[(data["k"] == k) & (data["column"]=="AGEP")]["map_size"]]
        wagp_y = [ x + y for x,y in zip(wagp_y, agep_y)]
        agep_b = [ x * 1024 / num_rows for x in agep_y]
        wagp_b = [ x * 1024/ num_rows for x in wagp_y]
        ax.bar([mp[k]], wagp_y, color='r',label="WAGP", width=10)
        ax.bar([mp[k]], agep_y, color='b',label="AGEP", width=10)
        ax2.bar([mp[k]], wagp_b, color='r',label="WAGP", width=10)
        ax2.bar([mp[k]], agep_b, color='b',label="AGEP", width=10) 
        if legend:
            legend = False
            ax.legend(fontsize=f, loc="upper right", frameon=False)
            ax2.legend(fontsize=f, loc="upper right", frameon=False)
        ax.set_xlabel("k", fontsize=f)
        ax.set_ylabel("Map size (KiB)", fontsize=f)
        #fig.suptitle("Index Size vs Size of the buckets", fontsize=f)
        ax2.set_xlabel("k", fontsize=f)
        ax2.set_ylabel("Bytes per tuple", fontsize=f)
        #fig2.suptitle("Bytes per tuple vs Size of the buckets", fontsize=f)
    ax.set_xticks([x for x in mp.values()])
    ax.set_xticklabels([x for x in ax.get_xticks()], fontsize=f)
    ax.set_ylim(ax.get_ylim()[0], 95)
    ax.set_yticklabels([int(x) for x in ax.get_yticks()], fontsize=f)
    fig.savefig(os.path.join(results, "Stacked_Num_Index_Size.pdf"))
    ax2.set_xticks([x for x in [10, 25, 50, 75, 100]])
    ax2.set_xticklabels([x for x in ax.get_xticks()], fontsize=f)
    ax2.set_yticklabels(["{:.3f}".format(x) for x in ax2.get_yticks()], fontsize=f)
    fig2.savefig(os.path.join(results, "Stacked_Num_Index_Bytes.pdf"))

    sec = ax.secondary_yaxis('right', functions=(abs_to_bpt, bpt_to_abs))
    sec.set_ylabel("Bytes per tuple", fontsize=f)
    fig.savefig(os.path.join(results,"Stacked_Num_All.pdf"))

    sec.set_yticklabels(["%.2f" % x for x in sec.get_yticks()], fontsize=f)
    fig.set_size_inches(6.4, 3.5)
    fig.set_tight_layout({"pad": .0})
    fig.savefig(os.path.join(results,"Stacked_Num_All.pdf"))


def plot_cat():
    num_rows = 3239553
    fig, ax = plt.subplots()
    fig2, ax2 = plt.subplots()
    plt.gcf().subplots_adjust(bottom=0.15, left=0.2)
    data = pd.read_csv(os.path.join(results,"size_cat_usa2019.csv"))
    f = 15
    data_size = len(data.index)
    legend = True
    for k,color,m in zip([100, 75, 50, 25, 10],["#7C5200", "#C4DF10", "m", "r", "cyan"], ["s", "o", "^", "D", "*"]):
        occp_y = [x / 1024 for x in data[(data["k"] == k) & (data["column"]=="OCCP")]["map_size"]]
        st_y = [x / 1024 for x in data[(data["k"] == k) & (data["column"]=="ST")]["map_size"]]
        occp_y = [ x + y for x,y in zip(occp_y, st_y)]
        occp_b = [ x * 1024 / num_rows for x in occp_y]
        st_b = [ x * 1024 / num_rows for x in st_y]
        ax.bar([k], occp_y, color='r',label="OCCP", width=10)
        ax.bar([k], st_y, color='b',label="ST", width=10)
        ax2.bar([k], occp_b, color='r',label="OCCP", width=10)
        ax2.bar([k], st_b, color='b',label="ST", width=10)
        if legend:
            legend = False
            ax.legend(fontsize=f, loc="upper right", frameon=False)
            ax2.legend(fontsize=f, loc="upper right", frameon=False)
        ax.set_xlabel("k", fontsize=f)
        ax.set_ylabel("Map size (KiB)", fontsize=f)
        #fig.suptitle("Index Size vs Size of the buckets", fontsize=f)
        ax2.set_xlabel("k", fontsize=f)
        ax2.set_ylabel("Bytes per tuple", fontsize=f)
        #fig2.suptitle("Bytes per tuple vs Size of the buckets", fontsize=f)
    ax.set_xticks([x for x in [10, 25, 50, 75, 100]])
    ax.set_xticklabels([x for x in ax.get_xticks()], fontsize=f)
    ax.set_yticklabels([int(x) for x in ax.get_yticks()], fontsize=f)
    fig.subplots_adjust(left=0.2)
    fig.savefig(os.path.join(results, "Stacked_Cat_Index_Size.pdf"))
    ax2.set_xticks([x for x in [10, 25, 50, 75, 100]])
    ax2.set_xticklabels([x for x in ax.get_xticks()], fontsize=f)
    ax2.set_yticklabels(["{:.2f}".format(x) for x in [0, 0.1, 0.2, 0.3, 0.4]], fontsize=f)
    fig2.savefig(os.path.join(results, "Stacked_Cat_Index_Bytes.pdf"))
    
    sec = ax.secondary_yaxis('right', functions=(abs_to_bpt, bpt_to_abs))
    sec.set_ylabel("Bytes per tuple", fontsize=f)
    fig.savefig(os.path.join(results,"Stacked_Cat_All.pdf"))

    sec.set_yticklabels(["%.2f" % x for x in sec.get_yticks()], fontsize=f)
    fig.set_size_inches(6.4, 3.5)
    fig.set_tight_layout({"pad": .0})
    fig.savefig(os.path.join(results,"Stacked_Cat_All.pdf"))

def plot_both():
    for t in ("", "GID"):
        num_rows = 3239553
        fig, ax = plt.subplots()
        plt.gcf().subplots_adjust(bottom=0.15, left=0.2)
        if t != "GID":
            data_num = pd.read_csv(os.path.join(results,"size_num_usa2019.csv"))
            data_cat = pd.read_csv(os.path.join(results,"size_cat_usa2019.csv"))
        else:
            data_num = pd.read_csv(os.path.join(results,"size_num_usa2019_gid.csv"))
            data_cat = pd.read_csv(os.path.join(results,"size_cat_usa2019_gid.csv"))
        f = 15
        data_size = len(data_num.index)
        legend = True
        mp = {10 : 25, 25:50, 50:75, 75:100, 100:125}
        for k,color,m in zip([100, 75, 50, 25, 10],["#7C5200", "#C4DF10", "m", "r", "cyan"], ["s", "o", "^", "D", "*"]):
            occp_y = [x / 1024 for x in data_cat[(data_cat["k"] == k) & (data_cat["column"]=="OCCP")]["map_size"]]
            st_y = [x / 1024 for x in data_cat[(data_cat["k"] == k) & (data_cat["column"]=="ST")]["map_size"]]
            occp_y = [ x for x,y in zip(occp_y, st_y)]
            occp_b = [ x * 1024 / num_rows for x in occp_y]
            st_b = [ x * 1024 / num_rows for x in st_y]
            ax.bar([mp[k] - 5], occp_y, color='#1f77b4', label="OCCP", width=5)
            ax.bar([mp[k]], st_y, color='#ff7f0e', label="ST",width=5)


            wagp_y = [x / 1024  for x in data_num[(data_num["k"] == k) & (data_num["column"]=="WAGP")]["map_size"]]
            agep_y = [x / 1024 for x in data_num[(data_num["k"] == k) & (data_num["column"]=="AGEP")]["map_size"]]
            wagp_y = [ x for x,y in zip(wagp_y, agep_y)]
            agep_b = [ x * 1024 / num_rows for x in agep_y]
            wagp_b = [ x * 1024/ num_rows for x in wagp_y]
            ax.bar([mp[k] + 5], wagp_y, color="#2ca02c", label="WAGP", width=5)
            ax.bar([mp[k] + 10], agep_y, color="#d62728", label="AGEP",width=5)

            if legend:
                legend = False
                handles, labels = ax.get_legend_handles_labels()
                order_map = { "OCCP": 1, "ST":2, "WAGP":3, "AGEP":4}
                entries = [x for x in zip(handles, labels)]
                entries.sort(key=lambda x: order_map[x[1]])
                ax.legend([x[0] for x in entries], [x[1] for x in entries], frameon=False,
                          bbox_to_anchor=(0,1.02,1,0.2), loc="lower left",
                          mode="expand", borderaxespad=0, ncol=4)

            ax.set_xlabel("k", fontsize=f)
            ax.set_ylabel("Map size (KiB)", fontsize=f)
        ax.set_xticks([x for x in mp.values()])
        ax.set_xticklabels([x for x in mp.keys()], fontsize=f)
        ax.set_yticklabels([int(x) for x in ax.get_yticks()], fontsize=f)
        if t == "GID":
           ax.set_ylim(top=2700)
        fig.subplots_adjust(left=0.2)
        fig.savefig(os.path.join(results, "Bar_Index_Size.pdf"))

        sec = ax.secondary_yaxis('right', functions=(abs_to_bpt, bpt_to_abs))
        sec.set_ylabel("Bytes per tuple", fontsize=f)
        fig.savefig(os.path.join(results,"Bar_Index_Size.pdf"))

        sec.set_yticklabels(["%.2f" % x for x in sec.get_yticks()], fontsize=f)
        fig.set_size_inches(6.4, 3.5)
        fig.set_tight_layout({"pad": .0})
        if t != "GID":
            fig.savefig(os.path.join(results,"Bar_Index_Size.pdf"))
        else:
            fig.savefig(os.path.join(results,"Bar_Index_Size_GID.pdf"))

def store_results(stored, map_size, k, column, plain_path):
        stored["column"].append(column)
        stored["k"].append(k)
        stored["dataset_size"].append(os.path.getsize(plain_path))
        stored["map_size"].append(map_size)
        stored["percentage"].append(map_size / os.path.getsize(plain_path))
        return stored


def calc_mappings(folder, dataset_format, categories, numericals, k_values):
    for t in ("", "GID"): 
            datasets = os.path.join(root, "datasets", folder)
            plain = os.path.join(root, "datasets", folder, f"{folder}.csv")
            stored = {"column": [],"map_size": [], "k": [], "dataset_size": [], "percentage": []}
            for cat in categories:
                if t != "GID":
                    os.system(f"make -C {root} test_categorical_mapping DATASETS={datasets} CATEGORICAL={cat}")
                else:
                    os.system(f"make -C {root} test_categorical_mapping_gid DATASETS={datasets} CATEGORICAL={cat}")
            for num in numericals:
                if t != "GID":
                    os.system(f"make -C {root} test_mapping DATASETS={datasets} COLUMN={num}")
                else:
                    os.system(f"make -C {root} test_mapping_gid DATASETS={datasets} COLUMN={num}")
            for k in k_values:
                    dataset = dataset_format % k
                    # check on disk size
                    map_size = 0
                    for c in categories:
                            sizes = []
                            for map_type in ["bitmap", "roaring", "set"]:
                                if t != "GID":
                                    sizes.append(os.path.getsize(f"{results}/{dataset}_{c}_{map_type}.pkl"))
                                else:
                                    sizes.append(os.path.getsize(f"{results}/{dataset}_{c}_{map_type}_GID.pkl"))
                            map_size += min(sizes)
                            stored = store_results(stored, min(sizes), k, c, plain)
                    for c in numericals:
                            sizes = []
                            to_check = [""] if c == "purchaseamount" else ["", "interval-tree"]
                            for map_type in to_check:
                                if t != "GID":
                                    map_path = f"{results}/{dataset}_{c}_{map_type}.pkl" if map_type != "" else f"{results}/{dataset}_{c}.pkl"
                                else:
                                    map_path = f"{results}/{dataset}_{c}_{map_type}_GID.pkl" if map_type != "" else f"{results}/{dataset}_{c}_GID.pkl"
                                sizes.append(os.path.getsize(map_path))
                            map_size += min(sizes)
                            stored = store_results(stored, min(sizes), k, c, plain)
                    stored = store_results(stored, map_size, k, "all", plain)
                    all_data = pd.DataFrame(data=stored)
                    if t!= "GID":
                        all_data[all_data.column.isin(categories)].to_csv(f"{results}/size_cat_{plain.split('/')[-1]}",index=False)
                        all_data[all_data.column.isin(numericals)].to_csv(f"{results}/size_num_{plain.split('/')[-1]}",index=False)
                    else:
                        all_data[all_data.column.isin(categories)].to_csv(f"{results}/size_cat_gid{plain.split('/')[-1]}",index=False)
                        all_data[all_data.column.isin(numericals)].to_csv(f"{results}/size_num_gid{plain.split('/')[-1]}",index=False)

def plot_times():
    f = 15
    colors = ["orange", "cyan", "purple"]
    w = 6
    cols = ["ST", "OCCP", "WAGP", "AGEP"]
    for c in cols:
            fig, ax = plt.subplots()
            plt.gcf().subplots_adjust(bottom=0.125, left=0.15)
            m = float("inf")
            legend = True
            for k in [25,50,75,100]:
                data = pd.read_csv(os.path.join(results,"times",f"usa2019_{k}_mapping_times.csv"))
                t = data[data["col"] == c]
                if len(t.index) == 3:
                        separators = range(-w, 2*w, w)
                else:
                        separators = range(int(-w/2), w, w)
                i = 0
                for l in separators:
                        ax.bar([k+l], t.iloc[i].time, color=colors[i], label=t.iloc[i].type, width=w)
                        if t.iloc[i].time < m:
                                m=t.iloc[i].time
                        i += 1
                if legend:
                        legend = False
                        fig.legend(frameon=False,
                        loc='upper center',
                        prop={'size': 12},
                        ncol=len(separators))
                ax.set_ylabel("Mapping times [s]", fontsize=f)
                ax.set_xlabel("k", fontsize=f)
                ax.set_xticks([25,50,75,100])
                ax.set_xticklabels(ax.get_xticks(), fontsize=f)
                if m < 0.01:
                        f_label = "{:.3f}"
                else:
                        f_label = "{:.1f}"
                ax.set_yticklabels([f_label.format(x) for x in ax.get_yticks()], fontsize=f)
                fig.savefig(os.path.join(results, f"usa2019_{c}_times.pdf"))



def get_conf(dict_name):
        """
        transactions = {
        "folder_name" : "transactions",
        "dataset_name" : "transactions_%s",
        "categories" : ["category", "company"],
        "numericals" : ["purchaseamount", "date"],
        "k" : [25]
        }
        """
        usa2019 = {
        "folder_name" : "usa2019",
        "dataset_name" : "usa2019_%s",
        "categories" : ["ST", "OCCP"],
        "numericals" : ["AGEP", "WAGP"],
        "k": [10, 25, 50, 75, 100]
        }
        """
        usa2018 = {
        "folder_name" : "usa2018",
        "dataset_name" : "usa2018_%s",
        "categories" : ["STATEFIP", "OCC"],
        "numericals" : ["AGE", "INCTOT"],
        "k": [25]
        }
        """
        all_dicts = {"usa2019": usa2019}
        return all_dicts.get(dict_name)


        

if __name__ == '__main__':
    
    parser = argparse.ArgumentParser(
        description='Retrieve the size of the mappings with varying K and ' +
                    'number of tuples.'
    )
    parser.add_argument('-p',
                        '--plot-only',
                        dest='plot_only',
                        action='store_true',
                        help='Only create the plots without calculating the maps')

    args = parser.parse_args()
    plot_only = args.plot_only

    root = os.path.realpath(os.path.join(__file__, *([os.path.pardir] * 3)))
    results = os.path.join(root, "test", "results", "mapping")

    if not plot_only:
        for data_name in [ "usa2019"]:
                conf = get_conf(data_name)
                calc_mappings(conf.get("folder_name"), conf.get("dataset_name"), conf.get("categories"), conf.get("numericals"), conf.get("k"))
    plot_num()
    plot_cat()
    plot_both()
    plot_times()
