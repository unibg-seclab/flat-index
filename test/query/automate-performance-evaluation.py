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
import getpass
import itertools
import os
import subprocess


POSTGRES = "postgresql://postgres:mysecretpassword@localhost:5432/postgres"
REDIS = "localhost:6379"


def add_network_emulation(backend, bandwidth, latency):
    print(f"Adding network emulation with a {bandwidth} Mbps bandwidth and " +
          f"{latency} ms latency...")
    subprocess.run([
        "docker", "exec", f"secure_index_{backend}_1", "tc", "qdisc", "add",
        "dev", "eth0", "root", "handle", "1:", "netem", "delay", f"{latency}ms"
    ])
    subprocess.run([
        "docker", "exec", f"secure_index_{backend}_1", "tc", "qdisc", "add",
        "dev", "eth0", "parent", "1:", "tbf", "rate", f"{bandwidth}mbit",
        "burst", "10mbit", "latency", "1ms"
    ])


def remove_network_emulation(backend):
    print(f"Removing network emulation...")
    subprocess.run([
        "docker", "exec", f"secure_index_{backend}_1", "tc", "qdisc", "del",
        "dev", "eth0", "root"
    ])


def get_baseline_basename(bandwidth, latency):
    return f"baseline-bandwidth={bandwidth}Mbit-latency={latency}ms.csv"


def get_wrapped_basename(i, k, serialization, compression, bandwidth, latency,
                         compact, suffix):
    return f"{i}-K={k}-serialization={serialization}" \
           f"-compression={compression}-bandwidth={bandwidth}Mbit" \
           f"-latency={latency}ms-{compact}{suffix}.csv"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Automate the run of multiple secure_index ' +
                    'configurations in order to evaluate their performance' +
                    'with varying network.'
    )
    parser.add_argument('column',
                        metavar='COLUMN',
                        nargs='+',
                        help='one or more columns on which to run queries')
    parser.add_argument('--back-end',
                        metavar='BACK-END',
                        nargs='+',
                        choices=["postgres", "redis"],
                        default=["postgres", "redis"],
                        help='one or more kind of back-end between postgres ' +
                             'and redis (default: both)')
    parser.add_argument('-b',
                        '--bandwidth',
                        metavar='BANDWIDTH',
                        nargs='+',
                        type=int,
                        default=[1000],
                        help='one or more bandwidths in Mbps (default: 1000 Mbps)')
    parser.add_argument('--compression',
                        metavar='ALGORITHM',
                        nargs='+',
                        choices=['none', 'lz4', 'snappy', 'zstd'],
                        default=['zstd'],
                        help='one or more compression algorithms: none, lz4, '
                             'snappy, zstd (default)')
    parser.add_argument('-c',
                        '--config',
                        metavar='CONFIG',
                        nargs='+',
                        help='one or more path to configuration files')
    parser.add_argument('-k',
                        metavar='K',
                        nargs='+',
                        type=int,
                        default=[50],
                        help='one or more bucket sizes (default: 50)')
    parser.add_argument('-l',
                        '--latency',
                        metavar='LATENCY',
                        nargs='+',
                        type=int,
                        default=[0],
                        help='one or more latencies in ms (default: 0 ms)')
    parser.add_argument('output',
                        metavar='OUPUT',
                        help='path of the directory where to store results')
    parser.add_argument('-p',
                        '--password',
                        help='password necessary to read the mapping')
    parser.add_argument('-q',
                        '--query',
                        metavar='QUERY',
                        help='path to the list of queries')
    parser.add_argument('-r',
                        '--reps',
                        metavar='REPETITIONS',
                        type=int,
                        default=3,
                        help='number of times each test should be repeated')
    parser.add_argument('-s',
                        '--sample-size',
                        metavar='SAMPLE_SIZE',
                        type=int,
                        default=1000,
                        help='size of the sample of queries to pick')
    parser.add_argument('--serialization',
                        metavar='FORMAT',
                        nargs='+',
                        choices=['json', 'msgpack', 'pickle'],
                        default=['json'],
                        help='serialization format: json (default), msgpack, '
                             'pickle')
    args = parser.parse_args()

    columns = args.column
    backends = args.back_end
    bandwidths = args.bandwidth
    compressions = args.compression
    configs = args.config
    ks = args.k
    latencies = args.latency
    output = args.output
    pw = args.password.encode("utf-8") if args.password else None
    queries = args.query
    reps = args.reps
    sample_size = args.sample_size
    serializations = args.serialization

    if not pw:
        pw = getpass.getpass("Password: ")

    root = os.path.realpath(os.path.join(__file__, *([os.path.pardir] * 3)))
    config = os.path.join(root, "config", "usa2019")
    datasets = os.path.join(root, "datasets", "usa2019")
    test = os.path.join(root, "test", "query")

    # Absolute config paths
    if configs:
        configs = [os.path.realpath(config) for config in configs]
    else:
        configs = [os.path.join(config, filename)
                   for filename in os.listdir(config)]

    # Absolute dataset paths
    plain = os.path.join(datasets, "usa2019.csv")
    anons = []
    for k in ks:
        anon = os.path.join(datasets, f"usa2019_{k}.csv")
        if not os.path.isfile(anon):
            raise Exception(f"{anon} does not exist.")
        anons.append(anon)

    output = os.path.realpath(output)
    if queries: queries = os.path.realpath(queries)

    # Absolute script paths
    query_script = os.path.join(test, "multi_column_query.py")
    run_query_script = os.path.join(test, "random_query.py")

    # Change current working directory to reuse make recipies
    os.chdir(root)

    print(f"[*] Columns: {columns}")
    subprocess.run(["make", "upload_plain", f"INPUT={plain}"])

    if not queries:
        # Run script to gather a list of queries
        print(f"[*] Retrieve a list of queries")
        queries = os.path.join(output, "queries.csv")
        subprocess.run([query_script, POSTGRES, *columns, queries])

    # Retrieve list of network configurations
    network_emulation_params = list(itertools.product(bandwidths, latencies))

    for bandwidth, latency in network_emulation_params:
        # TODO: Run random selection of queries on the plain dataset multiple
        #       times
        add_network_emulation("postgres", bandwidth, latency)
        # Run random selection of queries on the plain dataset
        basename = get_baseline_basename(bandwidth, latency)
        print(f"[*] Baseline: {basename}")
        baseline = os.path.join(output, basename)
        subprocess.run([
            run_query_script, "--plain", "--sample-size",
            str(sample_size), POSTGRES, queries, baseline
        ])
        remove_network_emulation("postgres")

    # Retrieve list of preprocessing configurations
    preprocess_params = itertools.product(backends, compressions, configs,
                                          zip(ks, anons), serializations)

    # TODO: Rewrite the following code to avoid all this code duplication
    for backend, compression, config, k_info, serialization in preprocess_params:
        compact, extension = os.path.splitext(os.path.basename(config))
        k, anon = k_info

        if backend == "postgres":
            # Single table with no flattening, gid and flattening
            # NOTE: Skip no flattening configuration to speed up tests
            if os.path.basename(config) == "no-gid.json":
                continue

            subprocess.run([
                "make", "preprocess", f"INPUT={plain}", f"ANONYMIZED={anon}",
                f"TYPE={config}", f"COMPRESSION={compression}",
                f"SERIALIZATION={serialization}"
            ])
            subprocess.run(["make", "upload"])

            for bandwidth, latency in network_emulation_params:
                add_network_emulation("postgres", bandwidth, latency)
                for i in range(reps):
                    # Run random selection of queries on the wrapped dataset
                    basename = get_wrapped_basename(i, k, serialization,
                                                    compression, bandwidth,
                                                    latency, compact, "")
                    print(f"[*] Wrapped: {basename}")
                    wrapped = os.path.join(output, basename)
                    subprocess.run([
                        run_query_script, "--compression", compression,
                        "--mapping", "mapping.enc", "--password", pw,
                        "--sample-size", str(sample_size),
                        "--serialization", serialization,
                        POSTGRES, queries, wrapped
                    ])
                remove_network_emulation("postgres")

            # NOTE: Skip configuration with two tables to speed up tests
            # # Main and mapping table with no flattening and flattening
            # # Skip configuration with gid
            # if os.path.basename(config) == "gid.json":
            #     continue

            # subprocess.run([
            #     "make", "preprocess_hybrid", f"INPUT={plain}",
            #     f"ANONYMIZED={anon}", f"TYPE={config}",
            #     f"COMPRESSION={compression}", f"SERIALIZATION={serialization}"
            # ])
            # subprocess.run(["make", "upload_hybrid"])

            # for bandwidth, latency in network_emulation_params:
            #     add_network_emulation("postgres", bandwidth, latency)
            #     for i in range(reps):
            #         # Run random selection of queries on the wrapped dataset
            #         basename = get_wrapped_basename(i, k, serialization,
            #                                         compression, bandwidth,
            #                                         latency, compact,
            #                                         "-indices")
            #         print(f"[*] Wrapped: {basename}")
            #         wrapped = os.path.join(output, basename)
            #         subprocess.run([
            #             run_query_script, "--compression", compression,
            #             "--mapping", "mapping_hybrid.enc", "--password", pw,
            #             "--representation", "mapping",
            #             "--sample-size", str(sample_size),
            #             "--serialization", serialization,
            #             POSTGRES, queries, wrapped
            #         ])
            #     remove_network_emulation("postgres")

        if backend == "redis" and os.path.basename(config) == "gid.json":
            # Key-value store with gid
            # Skip configuration with no flattening and flattening
            subprocess.run([
                "make", "preprocess_kv", f"INPUT={plain}",
                f"ANONYMIZED={anon}", f"OTHER_TYPE={config}",
                f"COMPRESSION={compression}", f"SERIALIZATION={serialization}"
            ])
            subprocess.run(["make", "upload_kv"])

            for bandwidth, latency in network_emulation_params:
                add_network_emulation("redis", bandwidth, latency)
                for i in range(reps):
                    # Run random selection of queries on the wrapped dataset
                    basename = get_wrapped_basename(i, k, serialization,
                                                    compression, bandwidth,
                                                    latency, compact, "-kv")
                    print(f"[*] Wrapped: {basename}")
                    wrapped = os.path.join(output, basename)
                    subprocess.run([
                        run_query_script, "--compression", compression,
                        "--kvstore", "--mapping", "mapping_kv.enc",
                        "--password", pw, "--sample-size", str(sample_size),
                        "--serialization", serialization,
                        REDIS, queries, wrapped
                    ])
                remove_network_emulation("redis")

        if backend == "redis" and os.path.basename(config) != "gid.json":
            # Main and mapping key-value stores with no flattening and
            # flattening
            # NOTE: Skip no flattening configuration to speed up tests
            if os.path.basename(config) == "no-gid.json":
                continue

            subprocess.run([
                "make", "preprocess_kv_mapping", f"INPUT={plain}",
                f"ANONYMIZED={anon}", f"TYPE={config}",
                f"COMPRESSION={compression}", f"SERIALIZATION={serialization}"
            ])
            subprocess.run(["make", "upload_kv_mapping"])

            for bandwidth, latency in network_emulation_params:
                add_network_emulation("redis", bandwidth, latency)
                for i in range(reps):
                    # Run random selection of queries on the wrapped dataset
                    basename = get_wrapped_basename(i, k, serialization,
                                                    compression, bandwidth,
                                                    latency, compact,
                                                    "-kv-indices")
                    print(f"[*] Wrapped: {basename}")
                    wrapped = os.path.join(output, basename)
                    subprocess.run([
                        run_query_script, "--compression", compression,
                        "--kvstore", "--mapping", "mapping_kv_mapping.enc",
                        "--password", pw, "--sample-size", str(sample_size),
                        "--serialization", serialization,
                        REDIS, queries, wrapped
                    ])
                remove_network_emulation("redis")
