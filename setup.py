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

from setuptools import setup


setup(
    name="secure_index",
    version="0.1.0",
    description="MOSAICrOWN secure index",
    install_requires=[
        "bitmap==0.0.7",
        "intervaltree==3.1.0",
        "pynacl==1.4.0",
        "pyroaring==0.3.3",
        "sqlparse==0.4.1",
    ],
    url="http://github.com/unibg-seclab/secure_index",
    author="UniBG Seclab",
    author_email="seclab@unibg.it",
    license="Apache",
    packages=[
        "secure_index",
        "secure_index.mapping",
        "secure_index.mapping._column_mapping",
    ],
    keywords="secure-index query rewriting",
)
