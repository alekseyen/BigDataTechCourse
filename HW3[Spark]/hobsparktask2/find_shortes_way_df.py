#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
import pyspark.sql.functions as f

spark = SparkSession.builder.appName('AlekseyenSpark02').master('yarn').getOrCreate()  # local[3] # yarn

NUM_PARTITIONS = 17  # number of partitions
edges_schema = StructType(fields=[
    StructField('end', IntegerType(), False),   #### !!! INTERSTING FACT
    StructField('start', IntegerType(), False)  #### FOLLOW in reverse order
])

edges = spark.read.csv('/data/twitter/twitter_sample.txt', sep='\t', schema=edges_schema).repartition(
    NUM_PARTITIONS).persist()

start_vertex, end_vertex = 12, 34
init_path = [[start_vertex, str(start_vertex)]]

paths_schema = StructType(fields=[
    StructField('end_vertex', IntegerType(), False),
    StructField('path', StringType(), False)
])

paths = spark.createDataFrame(init_path, schema=paths_schema).repartition(NUM_PARTITIONS)

for _ in range(int(1e5)):
    paths = paths.join(edges, paths.end_vertex == edges.start, 'inner')
    paths = paths.select(f.col('end').alias('end_vertex'),
                         f.concat_ws(',', paths['path'], paths['end']).alias('path'))

    if paths.filter(paths.end_vertex == end_vertex).count() > 0:
        break

_, path = paths.filter(paths.end_vertex == end_vertex).first()
print(path)
