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

from functools import partial

if __package__:
    from ._column_mapping.creation import create_bitmaps
    from ._column_mapping.creation import create_categorical_mapping
    from ._column_mapping.creation import create_interval_tree_mapping
    from ._column_mapping.creation import create_range_mapping
    from ._column_mapping.creation import create_roaring_bitmaps
    from ._column_mapping.creation import create_sets
else:
    from secure_index.mapping._column_mapping.creation import create_bitmaps
    from secure_index.mapping._column_mapping.creation import create_categorical_mapping
    from secure_index.mapping._column_mapping.creation import create_interval_tree_mapping
    from secure_index.mapping._column_mapping.creation import create_range_mapping
    from secure_index.mapping._column_mapping.creation import create_roaring_bitmaps
    from secure_index.mapping._column_mapping.creation import create_sets


CREATE = {
    "range": create_range_mapping,
    "interval-tree": create_interval_tree_mapping,
    "bitmap": partial(create_categorical_mapping, create_indexes=create_bitmaps),
    "roaring": partial(create_categorical_mapping, create_indexes=create_roaring_bitmaps),
    "set": partial(create_categorical_mapping, create_indexes=create_sets),
}


def create_heterogeneous_mapping(df,
                                 configs,
                                 key=None):
    mapping = {}
    types = {}
    is_gids = {}
    
    for column in configs.keys():
        # Extract configuration about the mapping of the current column
        config = configs[column]

        mapping_type = config["type"]
        keep_plain = config.get("plain", False)
        to_hash = config.get("hash", False)
        is_gid = config.get("gid", False)
        to_runtime = config.get("runtime", False)

        # avoid key override
        column_key = key if to_hash or to_runtime else None

        try:
            print(f"[*] Map {column} using {mapping_type}.")
            mapping[column] = CREATE[mapping_type](df,
                                                   column,
                                                   keep_plain=keep_plain,
                                                   to_hash=to_hash,
                                                   key=column_key,
                                                   use_gid=is_gid,
                                                   generate_at_runtime=to_runtime)

        except KeyError:
            raise Exception(f"{mapping_type} is not a valid type of mapping.")

        types[column] = mapping_type
        is_gids[column] = is_gid

    return mapping, types, is_gids
