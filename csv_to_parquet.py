import os
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

chunksize = 100_000
input_dir = '/data/in'
output_dir = '/data/out'

for entry in os.scandir(input_dir):
    if not (entry.is_file() and entry.path.endswith('csv')):
        continue

    print("Input: ", entry.path)
    csv_stream = pd.read_csv(entry.path, chunksize=chunksize, low_memory=False)
    output_location = os.path.join(output_dir, entry.name + '.parquet')
    print("Output: ", output_location)

    for i, chunk in enumerate(csv_stream):
        print("  chunk", i)
        if i == 0:
            # Guess the schema of the CSV file from the first chunk
            parquet_schema = pa.Table.from_pandas(df=chunk).schema
            # Open a Parquet file for writing
            parquet_writer = pq.ParquetWriter(output_location, parquet_schema, compression='snappy')
        # Write CSV chunk to the parquet file
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)

    parquet_writer.close()
