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


import multiprocessing
import os
import sys
from itertools import combinations
from itertools import tee
from multiprocessing import Pool
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def conver_data(elem):
    '''Conversion of each generalized element in order to simulate queries'''

    # The evaluation function in this moment works only for sets
    try:
        if elem[0] ==  '[':
            elem = elem[1:-1]
            sx, dx =  elem.split('-')
            if elem[-1] == ')':
                dx -= 1
            return [int(float(sx)), int(float(dx))]

    except:
        # Conversion of not generalized elements treated as int by pandas
        # Suppose it is an integer
        try:
            return int(float(elem))
            # Or return the string
        except:
            return elem

    # Conversion of sets
    if elem[0] == '{':
        # Use case specific solution to convert string elements
        if '>' in elem or '<' in elem:
            a =  set([ x for x in elem[1:-1].split(',')])
            return a
        # General solution to convert integer sets
        else:
            return set(eval(elem))
    # Conversion of not generalized elements treated as string by pandas
    else:
        # Suppose it is an integer
        try:
            return int(float(elem))
        # Or return the string
        except:
            return elem


def get_configuration(config):

    attribute_dict = {
        'usa_2018': ["STATEFIP", "AGE", "OCC", "INCTOT"],
        'usa_2019': ["ST", "AGEP", "OCCP", "WAGP"],
    }

    dataset_dict = {
        'usa_2018': f"{datasets}/usa2018/usa2018.csv",
        'usa_2019': f"{datasets}/usa2019/usa2019.csv",
    }

    basename_dict = {
        'usa_2018': f"{datasets}/usa2018/usa2018",
        'usa_2019': f"{datasets}/usa2019/usa2019",
    }

    k_dict = {
        'usa_2018': [25],
        'usa_2019': [25],
    }

    l_dict = {
        'usa_2018': [""],
        'usa_2019': [""],
    }
    sensitive_dict = {
        'usa_2018': "",
        'usa_2019': "",
    }
    range_dict = {
        'usa_2018': [],
        'usa_2019': ["AGEP"],
    }

    return attribute_dict[config], dataset_dict[config], basename_dict[config], k_dict[config], l_dict[config], sensitive_dict[config], range_dict.get(config, [])


def check_element(x, elem):
    to_return = False
    if type(x) is set:
        to_return = elem in x

    elif type(x) is list:
        sx = x[0]
        dx = x[1]
        to_return = elem >= sx and elem <= dx
    else:
        to_return = x == elem
    return to_return

def check_range(x, range_to_check):
    if type(x) is set:
        return len(x.intersection(range_to_check)) > 0

    if type(x) is list:
        other_range = set(range(x[0], x[1] + 1))
        return len(other_range.intersection(range_to_check)) > 0

    return x in range_to_check


def test_ranges(ranges):
    prev = None
    to_recover = None
    correct_nums = []
    mult_nums = []
    f_pos_rates = []
    ranges, r_copy = tee(ranges)
    chunck_size = sum(1 for r in r_copy)
    act = 0
    count = 0
    act_r = []
    values = set(df[column].unique())
    df_size = len(df.index)
    for r in ranges:
        count += 1
        if count % 100 == 0:
            print(f"Process {os.getpid()}:({count}/{chunck_size})")
        c = len(df[df[column].isin(range(r[0],r[1] + 1))].index)
        if c / df_size < 20:
            act += 1
            act_r.append((r,c))
    count = 0
    covered_range = []
    for r,c in sorted(act_r, key= lambda x : x[0][0]):

        count += 1
        if count % 100 == 0:
            print(f"Process {os.getpid()}:({count}/{act})")
        to_check = range(r[0],r[1] + 1)
        intersect = values.intersection(to_check)
        covered_range.append(len(intersect)/len(values))
        if prev is None or r[0] != prev:
            prev = r[0]
            to_recover = index_map[r[0]] if r[0] in index_map else pd.Index([])

        # This union works because the combinations are ordered
        to_recover = to_recover.union(index_map[r[1]], sort=False) if r[1] in index_map else to_recover
        correct_num_rows = c
        indexed_num_rows = len(to_recover)
        if correct_num_rows == 0:
            continue

        correct_nums.append(correct_num_rows)
        mult_nums.append(indexed_num_rows / correct_num_rows)
        f_pos_rates.append((indexed_num_rows -correct_num_rows) / indexed_num_rows)
    return correct_nums, mult_nums, f_pos_rates, covered_range


def parent_folder_creation(path):
    output_path = path
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_path


def file_creation_csv(path, filename, df):
    output_path = parent_folder_creation(path)
    pd.DataFrame(data=df).to_csv(f"{output_path}/{filename}", index=False)


def file_creation_plot(path, filename, plt):
    output_path = parent_folder_creation(path)
    plt.savefig(f"{output_path}/{filename}")


if __name__ == '__main__':
    root = os.path.realpath(os.path.join(__file__, "..", ".."))
    test = os.path.join(root, "test")
    datasets = os.path.join(root, "datasets")
    results = os.path.join(test, "results", "preliminary-analysis")
    selectivity = os.path.join(test, "results", "query", "selectivity_files")

    config = sys.argv[1]
    attributes, dataset, basename, k_params, l_params, sensitive, range_attributes = get_configuration(config)
    counters = {}
    global df
    # Read the raw dataset
    df = pd.read_csv(dataset)

    output_path = f"{results}/tables/simple/{config}"
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    for column in attributes:
        # for each element in each column, count how many time it appears in the raw dataset
        counters[column] = df[column].value_counts()
        # Save in a csv: element, # of occurrences, % of occurrence over the entire dataset
        simple_data = {'element': counters[column].index, 'size':counters[column], 'percentage': [x / sum(counters[column]) for x in counters[column]]}
        pd.DataFrame(data=simple_data).to_csv(f'{output_path}/value_count_{column}.csv', index=False)

    # Prepare the dataframe that will contain data about the multiplicative factor beetween puntual query on original data and anonimized data
    # The dataframe contains: K used in Mondrian, column name, mean, variance, max and min multiplicative factor
    statistics = {'k':[], 'l':[],'column':[], 'mean':[], 'w_mean':[], 'variance':[], 'w_variance':[], 'max':[], 'min': []}
    bucket_stat = {'k':[], 'l':[], 'portion': [], 'mean':[], 'w_mean':[], 'variance':[], 'false rate': [],'w_variance':[], 'max':[], 'min': []}
    query_stat = {'k':[], 'l':[], 'portion': [],'column':[], 'mean':[], 'w_mean':[], 'variance':[], 'false rate': [],'w_variance':[], 'max':[], 'min': []}
    query_stat_range = {'k':[], 'l':[], 'portion': [],'column':[], 'mean':[], 'w_mean':[], 'variance':[], 'false rate': [],'w_variance':[], 'max':[], 'min': []}
    bucket_query = {'k':[], 'l':[], 'portion': [], 'mean':[], 'w_mean':[], 'variance':[], 'false rate': [],'w_variance':[], 'max':[], 'min': []}
    for l in l_params:
        for k in k_params:
            global df_anon
            if l == "":
                df_anon = pd.read_csv(f"{basename}_{k}.csv")
            else:
                df_anon = pd.read_csv(f"{basename}_{k}_{l}.csv")
            counters = {}
            for column in attributes:
                counters[column] = df[column].value_counts()
            global_stat = {key:[[], []] for key in np.linspace(0,1,50,endpoint=False)}
            global_covered = {key:[[], [], [], []] for key in np.linspace(0,1,50,endpoint=False)}
            # Prepare dictionary: {column: {element: number of rows containing the key in generalized data} }
            freq = {}
            global index_maps
            index_maps = {}
            for column in attributes:
                freq[column] = {}
                datas = df_anon[column].map(conver_data)
                global index_map
                index_map = {}
                elem_count = 0
                for elem in counters[column].index:
                    elem_count += 1
                    print(f"{elem_count}/{len(counters[column].index)}")
                    freq[column][elem] = 0
                    data = datas[datas.apply(check_element, args=[elem])]
                    # A row is counted if it is equal to the original element of if the generalization contains it
                    index_map[elem] = data.index
                    freq[column][elem] += len(data.index)
                    for i in [(x, x +0.02) for x in np.linspace(0,1,50,endpoint=False)]:
                        if i[0] <= counters[column][elem] / len(df.index) < i[1]:
                            global_stat[i[0]][0].append(counters[column][elem])
                            global_stat[i[0]][1].append(freq[column][elem])


                index_maps[column] = index_map
                max_count = -1
                max_elem = -1
                # In order to calculate relative frequencies, recover the new mosINCTOTt frequent element
                for key, elem in freq[column].items():
                    if elem > max_count:
                        max_count = elem
                        max_elem = key

                # First plot: relative frequencies
                x = range(0, 100*len(counters[column].index), 10)
                fig, plots =  plt.subplots()
                # Original bars: count / (count of most frequent element in raw data)
                plots.bar([y - 17.5 for y in x[::10]], [x/counters[column].iloc[0] for x in counters[column]], 35, label='original')
                # K-anon bars: count / (count of the new most frequent element in k-anon data)
                plots.bar([y + 17.5 for y in x[::10]], [x/max_count for x in freq[column].values()], 35, label='k-anon')
                plots.set_xticks(x[::10])
                plots.set_xticklabels([str(x) for x in freq[column].keys()], rotation=90)
                fig.legend()
                fig.suptitle("Relative frequencies of the indexes")
                file_creation_plot(f"{results}/freq/{k}/{config}",
                       f"frequencies_{k}_{column}.pdf" if l == "" else f"frequencies_{k}_{l}_{column}.pdf",
                       plt)
                plt.close()

                # Second plot: multiplicative factors
                # Array of multiplicative factors: (element count in k-anon data) / (element count in raw data)
                multiplicative = [freq[column][x]/counters[column][x] for x in counters[column].index]
                fig, plots =  plt.subplots()
                plots.bar([y - 17.5 for y in x[::10]], multiplicative, 35, label='multiplicative factor')
                plots.set_xticks(x[::10])
                plots.set_xticklabels([str(x) for x in freq[column].keys()], rotation=90)
                fig.legend()
                fig.suptitle("Multiplicative worsening")
                file_creation_plot( f"{results}/worsening/{k}/{config}",
                       f"worsening_{k}_{column}.pdf" if l == "" else f"worsening_{k}_{l}_{column}.pdf",
                       plt)
                plt.close()
                # calculate mean and variance + weighted by number of occurrences of the element in the original data
                mean = sum(multiplicative)/ len(counters[column].index)
                w_mean =  sum([x*y for x,y in zip(multiplicative , counters[column])])/ sum(counters[column])
                var =  np.var(multiplicative)
                w_var = sum([y * ((x - w_mean)**2) for x,y in zip(multiplicative , counters[column])]) / sum(counters[column])

                statistics['k'].append(k)
                statistics['l'].append(l)
                statistics['column'].append(column)
                statistics['mean'].append(mean)
                statistics['w_mean'].append(w_mean)
                statistics['variance'].append(var)
                statistics['w_variance'].append(w_var)
                statistics['max'].append(max(multiplicative))
                statistics['min'].append(min(multiplicative))

                # Third plot: worsening. Comparison of non needed tuples vs actually requested
                fig, plots =  plt.subplots()
                plots.bar([y - 17.5 for y in x[::10]], [freq[column][x] for x in counters[column].index], 35, label='returned')
                plots.bar([y + 17.5 for y in x[::10]], counters[column], 35, label='requested')
                plots.set_xticks(x[::10])
                plots.set_xticklabels([str(x) for x in freq[column].keys()], rotation=90)
                fig.legend()
                fig.suptitle("Absolute worsening")
                file_creation_plot(f"{results}/tuples/{k}/{config}",
                       f"worsening_tuples_{k}_{column}.pdf" if l == "" else f"worsening_tuples_{k}_{l}_{column}.pdf",
                       plt)
                plt.close()

                # Create table containing the occurrences of each set created for column generalization
                class_counts = df_anon[column].value_counts()
                class_data = {'class': class_counts.index, 'hits': class_counts}
                file_creation_csv(f"{results}/tables/hits/{k}/{config}",
                       f"classes_{k}_{column}.csv" if l == "" else f"classes_{k}_{l}_{column}.csv",
                       class_data)

                # Range queries data
                if column in range_attributes:
                    correct_nums = []
                    mult_nums = []
                    ratio_nums = []
                    pos = []
                    all_ranges = [[] for i in range(0,30)]
                    to_combine = np.sort(df[column].unique())
                    ranges = combinations(to_combine, 2) # All possible range queries
                    prev = to_combine[0]
                    to_use = 0

                    # Divide for parallelization
                    for r in ranges:
                        if r[0] != prev:
                            to_use = to_use + 1 if to_use + 1 < len(all_ranges) else 0
                            prev = r[0]
                        all_ranges[to_use].append((r[0], r[1]))

                    with Pool(processes=multiprocessing.cpu_count()) as pool:
                        print("[*] Query simulation start")
                        result = pool.map(test_ranges, all_ranges)

                    mult_part = {key:[] for key in np.linspace(0,1,50,endpoint=False) }
                    correct_part = {key:[] for key in np.linspace(0,1,50,endpoint=False)}
                    pos_part = {key:[] for key in np.linspace(0,1,50,endpoint=False)}
                    covered_part = {key:[[], [], []] for key in np.linspace(0,1,100,endpoint=False)}
                    for correct, mult, positive_rates, covered_ranges in result:
                        correct_nums.extend(correct)
                        mult_nums.extend(mult)
                        pos.extend(positive_rates)
                        ratio_nums.extend(covered_ranges)
                        for c_num, m_num, p_num, r_num in zip(correct, mult, positive_rates, covered_ranges):
                            for i in [(x, x +0.02) for x in np.linspace(0,1,50,endpoint=False)]:
                                if i[0] <= c_num / len(df[column].index) < i[1]:
                                    mult_part[i[0]].append(m_num)
                                    correct_part[i[0]].append(c_num)
                                    pos_part[i[0]].append(p_num)
                            for i in [(x, x +0.01) for x in np.linspace(0,1,100,endpoint=False)]:
                                if i[0] <= r_num < i[1]:
                                    covered_part[i[0]][0].append(m_num)
                                    covered_part[i[0]][1].append(c_num)
                                    covered_part[i[0]][2].append(p_num)
                            for i in [(x, x +0.02) for x in np.linspace(0,1,50,endpoint=False)]:
                                if i[0] <= c_num / len(df[column].index) < i[1]:
                                    global_covered[i[0]][0].append(m_num)
                                    global_covered[i[0]][1].append(c_num)
                                    global_covered[i[0]][2].append(p_num)
                                    global_covered[i[0]][3].append(r_num)

                    mean = sum(mult_nums)/ len(mult_nums)
                    w_mean =  sum([x*y for x,y in zip(mult_nums , correct_nums)])/ sum(correct_nums)
                    var =  np.var(mult_nums)
                    w_var = sum([y * ((x - w_mean)**2) for x,y in zip(mult_nums , correct_nums)]) / sum(correct_nums)
                    query_stat['k'].append(k)
                    query_stat['l'].append(l)
                    query_stat['column'].append(column)
                    query_stat['mean'].append(mean)
                    query_stat['w_mean'].append(w_mean)
                    query_stat['variance'].append(var)
                    query_stat['w_variance'].append(w_var)
                    query_stat['max'].append(max(mult_nums))
                    query_stat['min'].append(min(mult_nums))
                    query_stat['false rate'].append(sum(pos) / len(pos))
                    query_stat['portion'].append('all')
                    for key in mult_part.keys():
                        mult_nums = mult_part[key]
                        correct_nums = correct_part[key]
                        if len(mult_nums) == 0:
                            continue
                        pos = pos_part[key]
                        mean = sum(mult_nums)/ len(mult_nums)
                        w_mean =  sum([x*y for x,y in zip(mult_nums , correct_nums)])/ sum(correct_nums)
                        var =  np.var(mult_nums)
                        w_var = sum([y * ((x - w_mean)**2) for x,y in zip(mult_nums , correct_nums)]) / sum(correct_nums)
                        query_stat['k'].append(k)
                        query_stat['l'].append(l)
                        query_stat['column'].append(column)
                        query_stat['mean'].append(mean)
                        query_stat['w_mean'].append(w_mean)
                        query_stat['variance'].append(var)
                        query_stat['w_variance'].append(w_var)
                        query_stat['max'].append(max(mult_nums))
                        query_stat['min'].append(min(mult_nums))
                        query_stat['false rate'].append(sum(pos)/ len(pos))
                        query_stat['portion'].append(key)
                    for key in covered_part.keys():
                        mult_nums = covered_part[key][0]
                        correct_nums = covered_part[key][1]
                        if len(mult_nums) == 0:
                            continue
                        pos = covered_part[key][2]
                        mean = sum(mult_nums)/ len(mult_nums)
                        w_mean =  sum([x*y for x,y in zip(mult_nums , correct_nums)])/ sum(correct_nums)
                        var =  np.var(mult_nums)
                        w_var = sum([y * ((x - w_mean)**2) for x,y in zip(mult_nums , correct_nums)]) / sum(correct_nums)
                        query_stat_range['k'].append(k)
                        query_stat_range['l'].append(l)
                        query_stat_range['column'].append(column)
                        query_stat_range['mean'].append(mean)
                        query_stat_range['w_mean'].append(w_mean)
                        query_stat_range['variance'].append(var)
                        query_stat_range['w_variance'].append(w_var)
                        query_stat_range['max'].append(max(mult_nums))
                        query_stat_range['min'].append(min(mult_nums))
                        query_stat_range['false rate'].append(sum(pos)/ len(pos))
                        query_stat_range['portion'].append(key)
                    file_creation_csv(f"{results}/tables/statistics/{config}", "stats_queries.csv", query_stat)
                    file_creation_csv(f"{results}/tables/statistics/{config}", "stats_queries_over_range.csv", query_stat_range)

                                
            for key in global_stat.keys():
                recovered = global_stat[key][1]
                correct_nums = global_stat[key][0]
                if len(recovered) == 0:
                    continue
                mult_nums = [i/j for i,j in zip(recovered, correct_nums)]
                pos = [ (i-j) / i for i,j in zip(recovered, correct_nums)]
                mean = sum(mult_nums)/ len(mult_nums)
                w_mean =  sum([x*y for x,y in zip(mult_nums , correct_nums)])/ sum(correct_nums)
                var =  np.var(mult_nums)
                w_var = sum([y * ((x - w_mean)**2) for x,y in zip(mult_nums , correct_nums)]) / sum(correct_nums)
                bucket_stat['k'].append(k)
                bucket_stat['l'].append(l)
                bucket_stat['mean'].append(mean)
                bucket_stat['w_mean'].append(w_mean)
                bucket_stat['variance'].append(var)
                bucket_stat['w_variance'].append(w_var)
                bucket_stat['max'].append(max(mult_nums))
                bucket_stat['min'].append(min(mult_nums))
                bucket_stat['false rate'].append(sum(pos)/ len(pos))
                bucket_stat['portion'].append(key)
            file_creation_csv(f"{results}/tables/statistics/{config}", "aggregate.csv", bucket_stat)
            for key in global_covered.keys():
                mult_nums = global_covered[key][0]
                correct_nums = global_covered[key][1]
                pos = global_covered[key][2]
                ratio = global_covered[key][3]
                if len(mult_nums) == 0:
                    continue
                mean = sum(mult_nums)/ len(mult_nums)
                w_mean =  sum([x*y for x,y in zip(mult_nums , correct_nums)])/ sum(correct_nums)
                var =  np.var(mult_nums)
                w_var = sum([y * ((x - w_mean)**2) for x,y in zip(mult_nums , correct_nums)]) / sum([y/z for y,z in zip(correct_nums,ratio)])
                bucket_query['k'].append(k)
                bucket_query['l'].append(l)
                bucket_query['mean'].append(mean)
                bucket_query['w_mean'].append(w_mean)
                bucket_query['variance'].append(var)
                bucket_query['w_variance'].append(w_var)
                bucket_query['max'].append(max(mult_nums))
                bucket_query['min'].append(min(mult_nums))
                bucket_query['false rate'].append(sum(pos)/ len(pos))
                bucket_query['portion'].append(key)
            # Create table for partitions actual size (can be more than k)
            if l == 2:
                df_grouped = df_anon[attributes].groupby(attributes)
            else:
                group_attributes = [x for x in attributes if x != sensitive]
                df_grouped = df_anon[group_attributes].groupby(group_attributes)
            data_groups = {'group': [], 'size': []}
            for row in df_grouped.groups:
                data_groups['group'].append(row)
                data_groups['size'].append(len(df_grouped.get_group(row).index))

            file_creation_csv(f"{results}/tables/partitions/{k}/{config}",
                                                           f"groups_sizes_{k}.csv" if l == "" else f"groups_sizes_{k}_{l}.csv", data_groups)

        file_creation_csv(f"{results}/tables/statistics/{config}", "stats.csv", statistics)
        file_creation_csv(f"{results}/tables/statistics/{config}", "stats_queries.csv", query_stat)
        file_creation_csv(f"{results}/tables/statistics/{config}", "stats_queries_over_range.csv", query_stat_range)
        file_creation_csv(f"{results}/tables/statistics/{config}", "aggregate.csv", bucket_stat)
        file_creation_csv(f"{results}/tables/statistics/{config}", "aggregate_queries.csv", bucket_query)
