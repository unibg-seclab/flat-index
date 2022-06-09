#!/bin/bash
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

# Fully automate performance evaluations

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

PYTHON_VERSION="$(python3 --version)"
VENVNAME=python-"${PYTHON_VERSION:7}"
VENV=$SCRIPT_DIR/../../.direnv/$VENVNAME
source $VENV/bin/activate

CONFIG=$SCRIPT_DIR/../../config/usa2019/gid.json
RESULTS=$SCRIPT_DIR/../results/query

echo -e "\n[*] EVALUATE QUERY PERFORMANCE WITH VARYING K"
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-AGEP.csv      $RESULTS/k/punctual-AGEP/       
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-AGEP.csv         $RESULTS/k/range-AGEP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP.csv      $RESULTS/k/punctual-OCCP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-OCCP.csv         $RESULTS/k/range-OCCP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-ST.csv        $RESULTS/k/punctual-ST/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-ST.csv           $RESULTS/k/range-ST/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-WAGP.csv      $RESULTS/k/punctual-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-WAGP.csv         $RESULTS/k/range-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP-WAGP.csv $RESULTS/k/punctual-OCCP-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-OCCP-WAGP.csv    $RESULTS/k/range-OCCP-WAGP/

echo -e "\n[*] EVALUATE QUERY PERFORMANCE WITH VARYNING K AND VARYNING BANDWIDTH"
$SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 100 1000 -c $CONFIG -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-WAGP.csv         $RESULTS/bandwidth/punctual-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 100 1000 -c $CONFIG -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-WAGP.csv            $RESULTS/bandwidth/range-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 100 1000 -c $CONFIG -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP.csv         $RESULTS/bandwidth/punctual-OCCP/
$SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 100 1000 -c $CONFIG -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP-WAGP.csv    $RESULTS/bandwidth/punctual-OCCP-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 100 1000 -c $CONFIG -k 10 25 50 75 100 -l 20 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-OCCP-WAGP.csv       $RESULTS/bandwidth/range-OCCP-WAGP/

echo -e "\n[*] EVALUATE QUERY PERFORMANCE WITH K=50 AND VARYNING LATENCY"
$SCRIPT_DIR/automate-performance-evaluation.py -k 50 -l 50 100 150 200 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-WAGP.csv       $RESULTS/latency/punctual-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 50 -l 50 100 150 200 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-WAGP.csv          $RESULTS/latency/range-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 50 -l 50 100 150 200 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP.csv       $RESULTS/latency/punctual-OCCP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 50 -l 50 100 150 200 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP-WAGP.csv  $RESULTS/latency/punctual-OCCP-WAGP/
$SCRIPT_DIR/automate-performance-evaluation.py -k 50 -l 50 100 150 200 -p password -r 3 -s 1000 $SCRIPT_DIR/usa2019-range-OCCP-WAGP.csv     $RESULTS/latency/range-OCCP-WAGP/

# echo -e "\n[*] EVALUATE QUERY PERFORMANCE WITH VARYNING BANDWIDTH, SERIALIZATION FORMAT AND COMPRESSION ALGORITHM"
# $SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 --compression none lz4 snappy zstd -c $CONFIG -k 100 -l 20 -p password -r 3 --serialization json msgpack pickle -s 1000 $SCRIPT_DIR/usa2019-punctual-WAGP.csv      $RESULTS/compression/punctual-WAGP/
# $SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 --compression none lz4 snappy zstd -c $CONFIG -k 100 -l 20 -p password -r 3 --serialization json msgpack pickle -s 1000 $SCRIPT_DIR/usa2019-range-WAGP.csv         $RESULTS/compression/range-WAGP/
# $SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 --compression none lz4 snappy zstd -c $CONFIG -k 100 -l 20 -p password -r 3 --serialization json msgpack pickle -s 1000 $SCRIPT_DIR/usa2019-punctual-OCCP-WAGP.csv $RESULTS/compression/punctual-OCCP-WAGP/
# $SCRIPT_DIR/automate-performance-evaluation.py --back-end redis -b 1 10 --compression none lz4 snappy zstd -c $CONFIG -k 100 -l 20 -p password -r 3 --serialization json msgpack pickle -s 1000 $SCRIPT_DIR/usa2019-range-OCCP-WAGP.csv    $RESULTS/compression/range-OCCP-WAGP/
