.PHONY: addlicense all baseline baseline_subset clean datasets preprocess preprocess_hybrid preprocess_kv preprocess_kv_mapping preprocess_norm query query_hybrid query_kv query_kv_mapping query_norm run stop test test_mapping test_categorical_mapping test_performance test_performance_hybrid test_performance_kv test_performance_kv_mapping test_subset_performance test_subset_performance_hybrid test_subset_performance_kv test_subset_performance_kv_mapping update usa2018 usa2018_simulation usa2019 usa2019_simulation visualization

SHELL			:= /bin/bash
MAKE			:= make --no-print-directory
REQUIRED_BINS		:= docker docker-compose

# Literal to define whitespace
space=$() $()

# Make the venv path name matches the default name used by direnv
VENVNAME		:= $(subst P,p,$(subst $(space),-,$(shell python3 --version)))
VENV			:= $(PWD)/.direnv/$(VENVNAME)
VIRTUALENV		:= python3 -m venv
ACTIVATE		:= $(VENV)/bin/activate
PYTHON			:= $(VENV)/bin/python
PIP				:= $(PYTHON) -m pip
REQUIREMENTS		:= requirements.txt

LICENSE_TYPE		:= "apache"
LICENSE_HOLDER		:= "Unibg Seclab (https://seclab.unibg.it)"

# I/O for wrapping and range indexes
INPUT			:= datasets/usa2018/usa2018.csv
ANONYMIZED		:= datasets/usa2018/usa2018_25.csv
MAPPING			:= mapping.enc
MAPPING_HYBRID		:= mapping_hybrid.enc
MAPPING_NORM		:= mapping_norm.enc
MAPPING_KV		:= mapping_kv.enc
MAPPING_KV_MAP		:= mapping_kv_mapping.enc
OUTPUT			:= datasets/wrapped/usa2018.csv
OUTPUT_HYBRID		:= datasets/wrapped/hybrid/wrapped_with_mapping.csv
OUTPUT_NORM		:= datasets/wrapped/norm/wrapped_with_normalization.csv
OUTPUT_KV		:= datasets/wrapped/usa2018_kv.csv
OUTPUT_KV_MAP		:= datasets/wrapped/kv_map/wrapped_with_mapping.csv

# Type of mapping / configuration path recipes use by default
TYPE			:= config/usa2018/no-gid.json
OTHER_TYPE		:= config/usa2018/gid.json

# Serialization format and compression algorithm recipes use by default
SERIALIZATION		:= json
COMPRESSION		:= zstd

# Database URLs
POSTGRES_URL		:= postgresql://postgres:mysecretpassword@localhost:5432/postgres
REDIS_URL		:= localhost:6379

all: run

run: | preprocess query

### VENV ###
$(VENV): $(REQUIREMENTS) setup.py
	@ echo "[*] Creating virtual environment"
	@ test -d $(VENV) || $(VIRTUALENV) $(VENV)
	@ $(PIP) install --upgrade pip > /dev/null
	@ $(PIP) install -r $(REQUIREMENTS) > /dev/null
	@ $(PYTHON) setup.py install > /dev/null
	@ touch $(ACTIVATE)

update: $(VENV)
	$(PYTHON) setup.py install

clean:
	@ echo "[*] Removing python virtual environment"
	@ rm -rf $(VENV)
	@ rm -rf build/ dist/ *.egg-info/
	@ rm -rf __pycache__
	@ rm -rf secure_index/__pycache__
	@ rm -rf sec_index_tool/__pycache__
	@ echo "[*] Removing docker containers"
	docker-compose rm --force --stop -v
	@- rm -f .*.build


### LICENSING ###
addlicense:
	@ go get -u github.com/google/addlicense
	$(shell go env GOPATH)/bin/addlicense -c $(LICENSE_HOLDER) -l $(LICENSE_TYPE) -y 2022 .


### SUBMODULE ###
.submodule.build:
	@ echo -e "[*] Configure indexing submodule.\n"
	@ make _setup_submodule
	@ touch $@

_setup_submodule:
	@ git submodule update --init
	@ cd submodules/spark-mondrian; git checkout k-flat;
	@ $(eval configs := $(shell ls partitioning-config/))
	@ $(foreach config,$(configs), cp partitioning-config/$(config) submodules/spark-mondrian/distributed/config;)

### MANAGE DATASETS ###
datasets: | _usa2019 _transactions

_usa2019:
	@ $(eval datasets := $(shell ls datasets/usa2019/*.zip))
	@ $(foreach dataset,$(datasets),make -s $(basename $(dataset)).csv;)

datasets/usa2019/usa2019_%.csv:
	@ unzip -o -d datasets/usa2019 $(basename $@).zip

_transactions:
	@ $(eval datasets := $(shell ls -d datasets/transactions/transactions* | grep -v '\.csv'))
	@ $(foreach dataset,$(datasets),make -s $(basename $(dataset)).csv;)

datasets/transactions%.csv:
	@ cat $(basename $@)/$(notdir $(basename $@))_part.tgz_* | tar xz -C $(dir $@)

USA2018_URL="https://www2.census.gov/programs-surveys/acs/data/pums/2018/1-Year/csv_pus.zip"
usa2018: .submodule.build
	@ $(MAKE) _download URL=$(USA2018_URL) OUTPUT=datasets/usa2018/usa2018.csv
	@ $(MAKE) _k_flat INPUT=datasets/usa2018/usa2018.csv K="5 25"

USA2019_URL="https://www2.census.gov/programs-surveys/acs/data/pums/2019/1-Year/csv_pus.zip"
usa2019: .submodule.build
	@ $(MAKE) _download URL=$(USA2019_URL) OUTPUT=datasets/usa2019/usa2019.csv
	@ $(MAKE) _k_flat INPUT=datasets/usa2019/usa2019.csv K="10 25 50 75 100"

transactions: .submodule.build _transactions
	@ cp datasets/transactions/transactions.csv submodules/spark-mondrian/distributed/dataset
	@ $(MAKE) -C submodules/spark-mondrian/distributed transactions WORKERS=20 WORKER_MEMORY=5G DRIVER_MEMORY=15G
	@ cp submodules/spark-mondrian/distributed/anonymized/transactions.csv datasets/transactions/transactions_25.csv
	@ $(PYTHON) dataset/script/shuffle.py datasets/transactions/transactions_25.csv

_download: $(VENV)
	wget $(URL)
	unzip -o csv_pus.zip -d unzipped
	$(PYTHON) datasets/script/build_dataset.py unzipped $(OUTPUT)
	rm -rf csv_pus.zip unzipped

WORKERS := 4
_k_flat:
	@ cp $(INPUT) submodules/spark-mondrian/distributed/dataset/$(notdir $(INPUT))
	@ $(foreach k,$(K), \
			echo "[*] RUNNING PARTITION ALGORITHM ON $(INPUT) WITH K=$(k)..."; \
	    	$(MAKE) -C submodules/spark-mondrian/distributed $(basename $(notdir $(INPUT))) K=$(k) CONFIG=$(basename $(notdir $(INPUT))) WORKERS=$(WORKERS); \
        	cp submodules/spark-mondrian/distributed/anonymized/$(notdir $(INPUT)) $(basename $(INPUT))_$(k).csv; \
			$(PYTHON) dataset/script/shuffle.py $(basename $(INPUT))_$(k).csv;)


### SIMULATIONS ###
usa2018_simulation: $(VENV)
	$(PYTHON) test/preliminary_analysis.py usa_2018

usa2019_simulation: $(VENV) _usa2019
	$(PYTHON) test/preliminary_analysis.py usa_2019


### CREATE MAPPING AND WRAP DATASET ACCORDINGLY ###
# These targets by default run our preprocessing on the usa2018 dataset, but
# by overwriting the default values of ANONYMIZED, INPUT and TYPE you can
# configure it as you wish
preprocess $(MAPPING) $(OUTPUT): $(VENV)
	@ echo -e "\n[*] CREATE GENERALIZATIONS TO TOKENS MAP"
	$(PYTHON) script/create_mapping.py --enc --password password --type $(TYPE) $(ANONYMIZED) $(MAPPING)
	@ echo -e "\n[*] WRAP DATASET"
	$(PYTHON) script/wrap.py --compression $(COMPRESSION) --pad --password password --serialization $(SERIALIZATION) $(INPUT) $(ANONYMIZED) $(MAPPING) $(OUTPUT)

preprocess_kv $(MAPPING_KV) $(OUTPUT_KV): $(VENV)
	@ echo -e "\n[*] CREATE GENERALIZATIONS TO TOKENS MAP"
	$(PYTHON) script/create_mapping.py --enc --password password --type $(OTHER_TYPE) $(ANONYMIZED) $(MAPPING_KV)
	@ echo -e "\n[*] WRAP DATASET"
	$(PYTHON) script/wrap.py --compression $(COMPRESSION) --kvstore --pad --password password --serialization $(SERIALIZATION) $(INPUT) $(ANONYMIZED) $(MAPPING_KV) $(OUTPUT_KV)

preprocess_kv_mapping $(MAPPING_KV_MAP) $(OUTPUT_KV_MAP): $(VENV)
	@ echo -e "\n[*] CREATE GENERALIZATIONS TO TOKENS MAP"
	$(PYTHON) script/create_mapping.py --enc --password password --type $(TYPE) $(ANONYMIZED) $(MAPPING_KV_MAP)
	@ echo -e "\n[*] WRAP DATASET"
	$(PYTHON) script/wrap.py --compression $(COMPRESSION) --kvstore --mapping --pad --password password --serialization $(SERIALIZATION) $(INPUT) $(ANONYMIZED) $(MAPPING_KV_MAP) $(OUTPUT_KV_MAP)

preprocess_hybrid $(MAPPING_HYBRID) $(OUTPUT_HYBRID): $(VENV)
	@ echo -e "\n[*] CREATE GENERALIZATIONS TO TOKENS MAP"
	$(PYTHON) script/create_mapping.py --enc --password password --type $(TYPE) $(ANONYMIZED) $(MAPPING_HYBRID)
	@ echo -e "\n[*] CREATE MAPPING TABLES AND WRAP DATASET"
	$(PYTHON) script/wrap.py --compression $(COMPRESSION) -m --pad --password password --serialization $(SERIALIZATION) $(INPUT) $(ANONYMIZED) $(MAPPING_HYBRID) $(OUTPUT_HYBRID)

preprocess_norm $(MAPPING_NORM) $(OUTPUT_NORM): $(VENV)
	@ echo -e "\n[*] CREATE GENERALIZATIONS TO TOKENS MAP"
	$(PYTHON) script/create_mapping.py --enc --password password --type $(TYPE) $(ANONYMIZED) $(MAPPING_NORM)
	@ echo -e "\n[*] CREATE MAPPING TABLES AND WRAP DATASET"
	$(PYTHON) script/wrap.py --compression $(COMPRESSION) -n --pad --password password --serialization $(SERIALIZATION) $(INPUT) $(ANONYMIZED) $(MAPPING_NORM) $(OUTPUT_NORM)


### MANAGE DOCKER CONTAINERS ###
check_deps:
	$(foreach bin,$(REQUIRED_BINS),\
		$(if $(shell which $(bin)),,$(error Please install `$(bin)`)))

.postgres.build:
	@ echo -e "[*] Building PostreSQL database image.\n"
	docker-compose build postgres
	@ touch $@

postgres: check_deps .postgres.build
	@ echo -e "[*] Starting PostreSQL database.\n"
	docker-compose up -d postgres

.redis.build:
	@ echo -e "[*] Building Redis key-value data store image.\n"
	docker-compose build redis
	@ touch $@

redis: check_deps .redis.build
	@ echo -e "[*] Starting Redis key-value data store.\n"
	docker-compose up -d redis

stop: check_deps
	@ echo -e "[*] Shutting down databases.\n"
	docker-compose kill


### UPLOAD DATASET TO SERVER ###
upload_plain: $(VENV) postgres
	@ sleep 2
	@ echo -e "\n[*] UPLOAD PLAIN DATASET TO POSTGRESQL DATABASE"
	$(PYTHON) script/upload.py --name plain $(INPUT) $(POSTGRES_URL)

upload: $(VENV) $(OUTPUT) postgres
	@ echo -e "\n[*] UPLOAD DATASET TO POSTGRESQL DATABASE"
	$(PYTHON) script/upload.py $(OUTPUT) $(POSTGRES_URL)

upload_kv: $(VENV) $(OUTPUT_KV) redis
	@ echo -e "\n[*] FLUSH REDIS DATABASE"
	echo "FLUSHALL" | nc -q 0 localhost 6379
	@ echo -e "\n[*] UPLOAD DATASET TO REDIS"
	$(PYTHON) script/upload.py --kvstore $(OUTPUT_KV) $(REDIS_URL)

upload_kv_mapping: $(VENV) $(OUTPUT_KV_MAP) redis
	@ echo -e "\n[*] FLUSH REDIS DATABASE"
	echo "FLUSHALL" | nc -q 0 localhost 6379
	@ echo -e "\n[*] UPLOAD MAPPINGs AND DATASET TO REDIS"
	@ $(eval names := $(shell ls $(dir $(OUTPUT_KV_MAP))))
	@ $(foreach name,$(names), $(PYTHON) script/upload.py --kvstore --name $(basename $(name)) "$(dir $(OUTPUT_KV_MAP))$(name)" $(REDIS_URL);)

upload_hybrid: $(VENV) $(OUTPUT_HYBRID) postgres
	@ echo -e "\n[*] UPLOAD MAPPING TABLE AND DATASET TO POSTGRESQL DATABASE"
	@ $(eval tables := $(shell ls $(dir $(OUTPUT_HYBRID))))
	@ $(foreach table,$(tables), $(PYTHON) script/upload.py --name $(basename $(table)) "$(dir $(OUTPUT_HYBRID))$(table)" $(POSTGRES_URL);)

upload_norm: $(VENV) $(OUTPUT_NORM) postgres
	@ echo -e "\n[*] UPLOAD MAPPING TABLEs AND DATASET TO POSTGRESQL DATABASE"
	@ $(eval tables := $(shell ls $(dir $(OUTPUT_NORM))))
	@ $(foreach table,$(tables), $(PYTHON) script/upload.py --name $(basename $(table)) "$(dir $(OUTPUT_NORM))$(table)" $(POSTGRES_URL);)


### QUERY SERVER ###
# These targets run a series of example queries against the usa2018 dataset,
# make sure to previously run the preprocessing stage accordingly
query: $(MAPPING) upload
	@ echo -e "\n[*] RUN SOME SIMPLE QUERY EXAMPLES ON POSTGRES"
	$(PYTHON) example/query.py --compression $(COMPRESSION) --password password --serialization $(SERIALIZATION) $(MAPPING) $(POSTGRES_URL)

query_kv: $(MAPPING_KV) upload_kv
	@ echo -e "\n[*] RUN SOME SIMPLE QUERY EXAMPLES ON REDIS"
	$(PYTHON) example/query.py --compression $(COMPRESSION) --kvstore --password password --serialization $(SERIALIZATION) $(MAPPING_KV) $(REDIS_URL)

query_kv_mapping: $(MAPPING_KV_MAP) upload_kv_mapping
	@ echo -e "\n[*] RUN SOME SIMPLE QUERY EXAMPLES ON REDIS USING MULTIPLE INDICES"
	$(PYTHON) example/query.py --compression $(COMPRESSION) --kvstore --password password --serialization $(SERIALIZATION) $(MAPPING_KV_MAP) $(REDIS_URL)

query_hybrid: $(MAPPING_HYBRID) upload_hybrid
	@ echo -e "\n[*] RUN SOME SIMPLE QUERY EXAMPLES"
	$(PYTHON) example/query.py --compression $(COMPRESSION) --password password -r mapping --serialization $(SERIALIZATION) $(MAPPING_HYBRID) $(POSTGRES_URL)

query_norm: $(MAPPING_NORM) upload_norm
	@ echo -e "\n[*] RUN SOME SIMPLE QUERY EXAMPLES"
	$(PYTHON) example/query.py --compression $(COMPRESSION) --password password -r normalization --serialization $(SERIALIZATION) $(MAPPING_NORM) $(POSTGRES_URL)


### PAPER TESTS ###
test: datasets .submodule.build $(VENV)
	@ echo -e "\n[*] EVALUATE TOKEN COLLISIONS WITH VARYING SIZE OF THE TOKENS"
	$(PYTHON) test/collision/test_token_collisions.py --token-sizes 1 2 3 4 --token-numbers 1 2 3 4 5 6 7 8 9 10 20 30 40 50 60 70 80 90 100 200 300 400 500 600 700 800 900 1000 2000 3000 4000 5000 6000 7000 8000 9000 10000 20000 30000 40000 50000 60000 70000 80000 90000 100000 200000 300000 400000 500000 600000 700000 800000 900000
	@ echo -e "\n[*] EVALUATE SIZE OF THE MAPPINGS WITH VARYING K AND NUMBER OF TUPLES"
	$(PYTHON) test/mapping/index_size_vs_k.py
	$(PYTHON) test/mapping/index_size_analysis.py
	@ echo -e "\n[*] EVALUATE QUERY PERFORMANCE"
	test/query/gather-performance-evaluations.sh


### PAPER FIGURES ###
visualization: _usa2019 $(VENV)
	@ echo -e "\n[*] PRODUCE FIGURES SHOWING INDICES DISTRIBUTION IN test/results/indices-distribution"
	$(MAKE) preprocess INPUT=datasets/usa2019/usa2019.csv ANONYMIZED=datasets/usa2019/usa2019_25.csv TYPE=config/usa2019/runtime.json
	$(PYTHON) test/indices_distribution.py -k 25 datasets/usa2019/usa2019.csv datasets/usa2019/usa2019_25.csv datasets/wrapped/usa2018.csv
	@ echo -e "\n[*] PRODUCE FIGURES SHOWING TOKEN COLLISIONS IN test/results/collision"
	$(PYTHON) test/collision/plot_token_collisions.py
	@ echo -e "\n[*] PRODUCE FIGURES SHOWING INDEX SIZE IN test/results/mapping"
	$(PYTHON) test/mapping/index_size_vs_k.py --plot-only
	$(PYTHON) test/mapping/index_size_analysis.py --plot-only
	@ echo -e "\n[*] PRODUCE FIGURES SHOWING PERFORMANCE EVALUATIONS IN test/results/query/images/*"
	test/query/produce-performance-evaluation-figures.sh


### TEST MAPPINGS ###
DATASETS	:= datasets/usa2018
PORTION     := 0

COLUMN		:= AGE
test_mapping: $(VENV)
	@ $(eval datasets := $(shell ls $(DATASETS)))
	@ echo -e "\n[*] RANGE MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(COLUMN)" -t range "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(COLUMN).pkl" -p $(PORTION);)
	@ echo -e "\n[*] INTERVAL TREE MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(COLUMN)" -t interval-tree "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(COLUMN)_interval-tree.pkl" -p $(PORTION);)

test_mapping_gid: $(VENV)
	@ $(eval datasets := $(shell ls $(DATASETS)))
	@ echo -e "\n[*] RANGE MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(COLUMN)" -t range "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(COLUMN)_GID.pkl" -p $(PORTION) -g;)
	@ echo -e "\n[*] INTERVAL TREE MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(COLUMN)" -t interval-tree "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(COLUMN)_interval-tree_GID.pkl" -p $(PORTION) -g;)
        

CATEGORICAL	:= STATEFIP
test_categorical_mapping: $(VENV)
	@ $(eval datasets := $(shell ls $(DATASETS)))
	@ echo -e "\n[*] BITMAP CATEGORICAL MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(CATEGORICAL)" -t bitmap "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(CATEGORICAL)_bitmap.pkl" -p $(PORTION);)
	@ echo -e "\n[*] SET CATEGORICAL MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(CATEGORICAL)" -t set "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(CATEGORICAL)_set.pkl" -p $(PORTION);)
	@ echo -e "\n[*] ROARING BITMAP CATEGORICAL MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(CATEGORICAL)" -t roaring "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(CATEGORICAL)_roaring.pkl" -p $(PORTION);)


test_categorical_mapping_gid: $(VENV)
	@ $(eval datasets := $(shell ls $(DATASETS)))
	@ echo -e "\n[*] BITMAP CATEGORICAL MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(CATEGORICAL)" -t bitmap "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(CATEGORICAL)_bitmap_GID.pkl" -p $(PORTION) -g;)
	@ echo -e "\n[*] SET CATEGORICAL MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(CATEGORICAL)" -t set "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(CATEGORICAL)_set_GID.pkl" -p $(PORTION) -g;)
	@ echo -e "\n[*] ROARING BITMAP CATEGORICAL MAPPINGS"
	@ $(foreach dataset,$(datasets), $(PYTHON) test/mapping/mapping.py -b -c "$(CATEGORICAL)" -t roaring "$(DATASETS)/$(dataset)" "test/results/mapping/$(basename $(dataset))_$(CATEGORICAL)_roaring_GID.pkl" -p $(PORTION) -g;)


### TEST QUERY PERFORMANCE ###
DELAY		:= 200
define run_with_network_emulation
	@ echo -e "\n[*] EMULATE A 1 Gbit NETWORK WITH A LATENCY OF $(DELAY) ms"
	@ docker exec secure_index_$(1)_1 tc qdisc add dev eth0 root handle 1: netem delay $(DELAY)ms
	@ docker exec secure_index_$(1)_1 tc qdisc add dev eth0 parent 1: tbf rate 1gbit burst 10mbit latency 1ms
	@ echo -e "\n[*] TEST QUERY PERFORMANCE"
	@ PYTHONIOENCODING=UTF-8 $(PYTHON) $(2)
	@ echo -e "\n[*] REMOVE NETWORK BANDWIDTH LIMIT AND LATENCY"
	@ docker exec secure_index_$(1)_1 tc qdisc del dev eth0 root
endef

BASELINE	:= test/results/query/baseline/usa2018
RESULTS		:= test/results/query

# These targets run queries against a single column, make sure to configure it
# accordingly to your previous preprocessing stage
baseline: $(VENV) upload_plain
	$(call run_with_network_emulation,postgres,test/query/query.py --plain $(POSTGRES_URL) $(COLUMN) $(BASELINE))

test_performance: $(VENV) upload_plain upload
	$(call run_with_network_emulation,postgres,test/query/query.py --baseline $(BASELINE) --compression $(COMPRESSION) --mapping $(MAPPING) --password password --serialization $(SERIALIZATION) $(POSTGRES_URL) $(COLUMN) $(RESULTS))

test_performance_hybrid: $(VENV) upload_plain upload_hybrid
	$(call run_with_network_emulation,postgres,test/query/query.py --baseline $(BASELINE) --compression $(COMPRESSION) --mapping $(MAPPING_HYBRID) --password password --representation mapping --serialization $(SERIALIZATION) $(POSTGRES_URL) $(COLUMN) $(RESULTS))

test_performance_kv: $(VENV) upload_plain upload_kv
	$(call run_with_network_emulation,redis,test/query/query.py --baseline $(BASELINE) --compression $(COMPRESSION) --kvstore $(REDIS_URL) --mapping $(MAPPING_KV) --password password --serialization $(SERIALIZATION) $(POSTGRES_URL) $(COLUMN) $(RESULTS))

test_performance_kv_mapping: $(VENV) upload_plain upload_kv_mapping
	$(call run_with_network_emulation,redis,test/query/query.py --baseline $(BASELINE) --compression $(COMPRESSION) --kvstore $(REDIS_URL) --mapping $(MAPPING_KV_MAP) --password password --serialization $(SERIALIZATION) $(POSTGRES_URL) $(COLUMN) $(RESULTS))

# These targets run queries against the usa2019 dataset, make sure to have run
# preprocess accordingly
PUNCTUAL := test/query/usa2019-punctual-WAGP.csv
RANGE := test/query/usa2019-range-WAGP.csv

baseline_subset: $(VENV) upload_plain
	$(call run_with_network_emulation,postgres,test/query/random_query.py --plain $(POSTGRES_URL) $(PUNCTUAL) $(BASELINE)/punctual.csv)
	$(call run_with_network_emulation,postgres,test/query/random_query.py --plain $(POSTGRES_URL) $(RANGE) $(BASELINE)/range.csv)

test_subset_performance: $(VENV) upload
	$(call run_with_network_emulation,postgres,test/query/random_query.py --compression $(COMPRESSION) --mapping $(MAPPING) --password password --serialization $(SERIALIZATION) $(POSTGRES_URL) $(PUNCTUAL) $(RESULTS)/punctual.csv)
	$(call run_with_network_emulation,postgres,test/query/random_query.py --compression $(COMPRESSION) --mapping $(MAPPING) --password password --serialization $(SERIALIZATION) $(POSTGRES_URL) $(RANGE) $(RESULTS)/range.csv)

test_subset_performance_hybrid: $(VENV) upload_hybrid
	$(call run_with_network_emulation,postgres,test/query/random_query.py --compression $(COMPRESSION) --mapping $(MAPPING_HYBRID) --password password --representation mapping --serialization $(SERIALIZATION) $(POSTGRES_URL) $(PUNCTUAL) $(RESULTS)/punctual.csv)
	$(call run_with_network_emulation,postgres,test/query/random_query.py --compression $(COMPRESSION) --mapping $(MAPPING_HYBRID) --password password --representation mapping --serialization $(SERIALIZATION) $(POSTGRES_URL) $(RANGE) $(RESULTS)/range.csv)

test_subset_performance_kv: $(VENV) upload_kv
	$(call run_with_network_emulation,redis,test/query/random_query.py --compression $(COMPRESSION) --kvstore --mapping $(MAPPING_KV) --password password --serialization $(SERIALIZATION) $(REDIS_URL) $(PUNCTUAL) $(RESULTS)/punctual.csv)
	$(call run_with_network_emulation,redis,test/query/random_query.py --compression $(COMPRESSION) --kvstore --mapping $(MAPPING_KV) --password password --serialization $(SERIALIZATION) $(REDIS_URL) $(RANGE) $(RESULTS)/range.csv)

test_subset_performance_kv_mapping: $(VENV) upload_kv_mapping
	$(call run_with_network_emulation,redis,test/query/random_query.py --compression $(COMPRESSION) --kvstore --mapping $(MAPPING_KV_MAP) --password password --serialization $(SERIALIZATION) $(REDIS_URL) $(PUNCTUAL) $(RESULTS)/punctual.csv)
	$(call run_with_network_emulation,redis,test/query/random_query.py --compression $(COMPRESSION) --kvstore --mapping $(MAPPING_KV_MAP) --password password --serialization $(SERIALIZATION) $(REDIS_URL) $(RANGE) $(RESULTS)/range.csv)
