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

# Direct to Parquet, using Sqoop

At the current point in time, I'm not sold on Sqoop's advantages; it requires a more complex toolchain (alleviated by Docker), and one of its key USPs from our point of view (schema type mapping) appears flakey, at least with Oracle.

## Setup:

You need JDBC drivers available:

```
$ ls /path/to/jdbc/
ojdbc6.jar
postgresql-42.2.13.jar
```

Use an image with Hadoop/Scoop already set up, mounting a destination directory:

```
$ docker run -v /path/to/jdbc/:/jdbc -v /path/to/dest/:/parquet_out/ -it dvoros/sqoop:latest
```

## Postgres

Within that container, point Scoop at the source DB. Use of `--direct` would require e.g. a PG install within the container.

```
# sqoop import --as-parquetfile --connect jdbc:postgresql://host.docker.internal:<port>/<db_name> --table <table_name> --username <user> --password <password> # careful!
[...]
```

Output can then be pulled out of the container:

```
# hdfs dfs -ls -R /user/root/<table_name>
[...]
# hdfs dfs -copyToLocal /user/root/<table_name>/*.parquet /parquet_out/<table_name>/ # crude, could FUSE mount...
```

...and checked over:

```
docker run -v $(pwd)/data/in:/data/in -v $(pwd)/data/out:/data/out csv_to_parquet:latest python
```

```
>>> import pandas as pd
>>> import pyarrow as pa
>>> import pyarrow.parquet as pq
>>> import glob

# This might be better doing the concatination as a reduction over the files, depending on Pandas behaviour...
>>> df = pd.concat([pd.read_parquet(f) for f in glob.glob('/data/out/<table_name>/*.parquet')], ignore_index=True)
```

## Oracle

Similar to Postgres above, but some oddities with schemas means things only seem to work when specifying query and parallel/serial options to Sqoop:

```
# sqoop import --as-parquetfile --connect jdbc:oracle:thin:@<host>:<port>:<SID> --query 'select * from <TABLE_NAME_UPCASE> where $CONDITIONS' --target-dir '/user/root/<table_name>' -m 1 --username <user> --password <password>
[...]
```

## Issues

### Epoch serialistation of dates

It would appear that there's a [known issue](https://community.cloudera.com/t5/Support-Questions/SQOOP-IMPORT-map-column-hive-ignored/td-p/45369/page/2) with dates/times and outputting to parquet; they get imported as millisecond-since-epoch values:

```
$ parquet-tools schema ed4f9bbf-7dbf-495a-afde-484b048d0737.parquet | grep 'DATE'
  optional int64 STARTDATE;
  optional int64 ENDDATE;
```

As far as I can tell, this is a limitation of the use of Avro by Sqoop for producing the parquet, and not correctly adding `LogicalType` annotations, which is how Parquet records a date being stored as an integer for example.

```
$ parquet-tools meta ed4f9bbf-7dbf-495a-afde-484b048d0737.parquet 2>&1 |\
                grep -oE 'parquet.avro.schema.*' | cut -d ' ' -f 3- | jq .fields
...
  {
    "name": "STARTDATE",
    "type": [
      "null",
      "long"
    ],
    "default": null,
    "columnName": "STARTDATE",
    "sqlType": "93"
  },
...
```

It's possible to perform this conversion after the fact, but that's not ideal:

```
>>> df.STARTDATE = pd.to_datetime(df.STARTDATE, unit='ms')
```

Equally, it's possible to override Sqoop's type map to a different primitive type, e.g. `String`:
```
$ sqoop import --map-column-java STARTDATE=String ...
```

I can't find a way of modifying the produce Parquet files' metadata to include the logical type.

### Repair of epoch dates

It's possible to use Pandas to re-emit Parquet with a tweaked schema. For example:

```
>>> epoch_df = pd.read_parquet('/data/in/epoch.parquet')
>>> epoch_df.STARTDATE = pd.to_datetime(epoch_df.STARTDATE, unit='ms').astype('datetime64[ms]')
>>> epoch_df.to_parquet('/data/out/fixed.parquet')
```

Observe the logical annotation now on `STARTDATE`, but not on `ENDDATE`:

```
parquet-tools meta fixed.parquet | grep 'STARTDATE\|ENDDATE'
STARTDATE:            OPTIONAL INT64 O:TIMESTAMP_MILLIS R:0 D:1
ENDDATE:              OPTIONAL DOUBLE R:0 D:1
```

and similarly in Python:

```
pd.read_parquet('/data/out/zicd_d/fixed.parquet').dtypes['STARTDATE':'ENDDATE']
STARTDATE    datetime64[ns]
ENDDATE             float64
```

In practice, one might want to iterate over emitted parquet chunks, and perform the same repair on each.

### Handling of NUMERIC and DECIMAL types from Oracle

In Avro/Parquet, these types are supposed to be handled logically - if not, they default to coming back as a `String`. In theory, this behaviour should be toggable by setting the following options:

```
-D sqoop.avro.decimal_padding.enable=true
-D sqoop.avro.logical_types.decimal.enable=true
-D sqoop.parquet.logical_types.decimal.enable=true
-D sqoop.avro.logical_types.decimal.default.precision=38
-D sqoop.avro.logical_types.decimal.default.scale=10
```

However, I had zero luck trying to get any of this working, despite evidence that the `OracleManager` was definitely being used.

The only practical workaround I could come up with was directly overriding the column mapping `--map-column-java EASTING=Integer`, which somewhat defeats the benefit of Sqoop.

Code refs:

```
src/java/org/apache/sqoop/orm/AvroSchemaGenerator.java:85 (Schema generate)
src/java/org/apache/sqoop/manager/ConnManager.java:189 (toAvroType)
src/java/org/apache/sqoop/manager/oracle/OracleUtils.java:89 (toAvroLogicalType)
src/java/org/apache/sqoop/orm/AvroSchemaGenerator.java:129 (toAvroSchema)
```
