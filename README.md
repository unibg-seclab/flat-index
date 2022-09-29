# Multi-Dimensional Flat Indexing for Encrypted Databases

This repository contains a Python library implementing the secure index
construction and its runtime usage, and a complete system showing
how to leverage our secure index in order to store encrypted databases
on PostgreSQL and Redis, and how to query them.

## Prerequisites

- [docker](https://docs.docker.com/get-docker/)
- [docker-compose](https://docs.docker.com/compose/install/)

## Workflow

To enable the flat secure indexing of an encrypted dataset, multiple steps
are necessary:

1. construction of the flat representation
2. construction of the maps on top of the flat representation
3. dataset wrapping
4. dataset outsourcing

Upon sending a query to the backend, the client:

1. parses it
2. uses the maps to rewrite it
3. leverages different server execution strategies based on the current
   setup

When receiving the response, the client:

1. decrypts the encrypted blocks pulled from the server
2. creates a temporary table in a local SQLite instance
3. filters spurious tuples by re-running the original query in SQLite
4. returns the results of the query as if it was run against a plaintext
   database

### Preprocessing

To construct the maps and use them to prepare the dataset for secure
outsourcing, run:

```shell
make preprocess
```

NOTE: multiple preprocessing targets exist, take a look into the Makefile
to have a complete view of the configurations available.

### Runtime execution of queries

To upload the dataset and query it run:

```shell
make query
```

NOTE: multiple query targets exist, depending on the preprocessing done
previously, take a look into the Makefile to have a complete view of the
configurations available.

## Reproduce experiments

The experiments can be reproduced with:

```shell
make test
```

NOTE: This takes few days to execute on our hardware. If you don't
have the time to reproduce the experiments you can still recreate our
figures starting from our experimental data as follows.

## Create experimental evaluation figures

In order to visualize the results of the experimental evaluation you
can run:

```shell
make visualization
```
