# CSV -> Parquet

## Overview

This repository contains a proof of concept for converting CSV data into the Parquet format.

It uses Python's `pandas` library to produce the Parquet files, which avoids the need for Hadoop to be running.

## Usage

First, build a tagged image:
```
docker build -t csv_to_parquet .
```
Then, put `.csv` files in `data/in/`, and run:
```
docker run -v $(pwd)/data/in:/data/in -v $(pwd)/data/out:/data/out csv_to_parquet:latest
```
Output `.parquet` files will appear in `data/out/`.

# Direct to Parquet

```
$ ls /path/to/jdbc/
ojdbc6.jar
postgresql-42.2.13.jar

$ docker run -v /path/to/jdbc/:/jdbc -v path/to/dest/:/parquet_out/ -it dvoros/sqoop:latest
# sqoop import --as-parquetfile --connect jdbc:postgresql://host.docker.internal:<port>/<db_name> --table <table_name> --username <user> --password <password> # careful!
[...]
# hdfs dfs -ls -R /user/root/<table_name>
[...]
# hdfs dfs -copyToLocal /user/root/<table_name>/*.parquet /parquet_out/ # crude, could FUSE mount...
