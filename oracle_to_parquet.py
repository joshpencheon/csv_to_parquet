import os
import sys

import cx_Oracle

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# cx_Oracle => PyArrow type map
type_map = {
    cx_Oracle.DB_TYPE_BFILE: pa.binary(),
    cx_Oracle.DB_TYPE_BINARY_DOUBLE: pa.float64(),
    cx_Oracle.DB_TYPE_BINARY_FLOAT: pa.float64(),
    cx_Oracle.DB_TYPE_BLOB: pa.binary(),
    cx_Oracle.DB_TYPE_CHAR: pa.string(),
    cx_Oracle.DB_TYPE_CLOB: pa.binary(),
    # cx_Oracle.DB_TYPE_CURSOR
    cx_Oracle.DB_TYPE_DATE: pa.timestamp('ms'),
    # cx_Oracle.DB_TYPE_INTERVAL_DS
    cx_Oracle.DB_TYPE_LONG: pa.string(),
    cx_Oracle.DB_TYPE_LONG_RAW: pa.binary(),
    cx_Oracle.DB_TYPE_NCHAR: pa.string(),
    cx_Oracle.DB_TYPE_NCLOB: pa.binary(),
    # cx_Oracle.DB_TYPE_NUMBER: pa.float64(), # could reflect on precision/scale
    cx_Oracle.DB_TYPE_NVARCHAR: pa.string(),
    # cx_Oracle.DB_TYPE_OBJECT
    cx_Oracle.DB_TYPE_RAW: pa.binary(),
    cx_Oracle.DB_TYPE_ROWID: pa.string(),
    cx_Oracle.DB_TYPE_TIMESTAMP: pa.timestamp('ms'),
    cx_Oracle.DB_TYPE_TIMESTAMP_LTZ: pa.timestamp('ms'),
    cx_Oracle.DB_TYPE_TIMESTAMP_TZ: pa.timestamp('ms'),
    cx_Oracle.DB_TYPE_VARCHAR: pa.string()
}

def arrow_type_for(cx_oracle_type, precision, scale):
    """Maps Oracle column type to an Array type

    Primarily uses the defined type_map, but in the case of Oracle
    NUMBER columns will map to int/float/bool depending on precision and scale.
    """

    if cx_oracle_type == cx_Oracle.DB_TYPE_NUMBER:
        if scale == 0:
            if precision == 1:
                return pa.bool_()
            else:
                return pa.int64()
        else:
            return pa.float64()
    else:
        return type_map.get(cx_oracle_type)


chunksize = 100_000
output_dir = 'data/out'

username = os.environ.get('USERNAME')
password = os.environ.get('PASSWORD')
database = os.environ.get('DATABASE') # "ip_or_host/db_name"
table = os.environ.get('TABLE')

connection = cx_Oracle.connect(username, password, database)
query = 'select * from %s' % table

with connection.cursor() as cursor:
    type_query = 'select * from (%s) where 1=0' % query
    cursor.execute(type_query)
    cx_oracle_types = cursor.description

print(cx_oracle_types)

mapped_cols = [(col[0], arrow_type_for(col[1], col[4], col[5])) for col in cx_oracle_types]
print(mapped_cols)

parquet_schema = pa.schema(mapped_cols)
print(parquet_schema)

for i, df in enumerate(pd.read_sql(query, connection, chunksize=chunksize)):
    print("  chunk", i)
    output_location = os.path.join(output_dir, ('query-chunk-%d.parquet' % i))
    print("Output: ", output_location)

    with pq.ParquetWriter(output_location, parquet_schema, compression='snappy') as parquet_writer:
        table = pa.Table.from_pandas(df, schema=parquet_schema)
        parquet_writer.write_table(table)
