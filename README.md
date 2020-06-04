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
docker run -it -v $(pwd)/data/in:/data/in -v $(pwd)/data/out:/data/out csv_to_parquet:latest
```
Output `.parquet` files will appear in `data/out/`.
