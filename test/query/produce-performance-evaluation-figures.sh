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

# Fully automate the generation of figures showing performance evaluations

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

PYTHON_VERSION="$(python3 --version)"
VENVNAME=python-"${PYTHON_VERSION:7}"
VENV=$SCRIPT_DIR/../../.direnv/$VENVNAME
source $VENV/bin/activate

echo $SCRIPT_DIR

echo -e "\n[*] PRODUCE FIGURES SHOWING SIZE OF THE QUERY RESULT WITH VARYING K"
$SCRIPT_DIR/performance_ad_hoc_punctual.py
$SCRIPT_DIR/performance_ad_hoc_range.py

# echo -e "\n[*] PRODUCE FIGURES SHOWING QUERY PERFORMANCE WITH VARYING SIZE OF THE QUERY, K=50 AND LATENCY=10ms"
# for path in $SCRIPT_DIR/../results/query/k/*; do
#     echo "[*] $( basename "$path" )"
#     $SCRIPT_DIR/performance_by_size.py --performance --size "$path"
# done

echo -e "\n[*] PRODUCE FIGURES SHOWING QUERY PERFORMANCE WITH VARYING K AND LATENCY=10ms"
for path in $SCRIPT_DIR/../results/query/k/*; do
    echo "[*] $( basename "$path" )"
    $SCRIPT_DIR/performance_by_K.py --performance --size "$path"
done

echo -e "\n[*] PRODUCE FIGURES SHOWING QUERY PERFORMANCE WITH VARYING K AND BANDWIDTH"
for path in $SCRIPT_DIR/../results/query/bandwidth/*; do
    echo "[*] $( basename "$path" )"
    $SCRIPT_DIR/performance_by_K_and_bandwidth.py "$path"
done

echo -e "\n[*] PRODUCE FIGURES SHOWING QUERY PERFORMANCE WITH K=50 AND VARYNING LATENCY"
for path in $SCRIPT_DIR/../results/query/latency/*; do
    echo "[*] $( basename "$path" )"
    $SCRIPT_DIR/performance_by_latency.py "$path"
done

# echo -e "\n[*] PRODUCE FIGURES SHOWING QUERY PERFORMANCE WITH VARYNING SERIALIZATION FORMAT, COMPRESSION ALGORITHM AND BANDWIDTH"
# for path in $SCRIPT_DIR/../results/query/compression/*; do
#     echo "[*] $( basename "$path" )"
#     $SCRIPT_DIR/performance_by_compression_and_bandwidth.py "$path"
# done


echo -e "\n[*] MOVE FIGURES TO test/results/query/images/*"
for path in $SCRIPT_DIR/../results/query/k/*; do
    mv $path/*.pdf "$SCRIPT_DIR/../results/query/images/$( basename "$path" )"
done
for path in $SCRIPT_DIR/../results/query/bandwidth/*; do
    mv $path/*.pdf "$SCRIPT_DIR/../results/query/images/$( basename "$path" )"
done
for path in $SCRIPT_DIR/../results/query/latency/*; do
    mv $path/*.pdf "$SCRIPT_DIR/../results/query/images/$( basename "$path" )"
done
# for path in $SCRIPT_DIR/../results/query/compression/*; do
#     mv $path/*.pdf "$SCRIPT_DIR/../results/query/images/$( basename "$path" )"
# done

echo -e "\n[*] CROP FIGURES"
for path in $SCRIPT_DIR/../results/query/images/*; do
    echo $path
    for filename in $path/*; do
        basename=$( basename "$filename" )
        if [[ $basename != cropped-* ]]; then
            pdfcrop "$filename" "$( dirname "$filename" )/cropped-$basename"
        fi
    done
done
