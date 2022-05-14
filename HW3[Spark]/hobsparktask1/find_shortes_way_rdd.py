#!/usr/bin/env python3

from pyspark import SparkContext, SparkConf


def parse_edge(s):
    user, follower = s.split("\t")
    return int(user), int(follower)


def step(item):
    prev_v, path, next_v = item[0], item[1][0], item[1][1]
    path += ',' + str(next_v)
    return next_v, path

NUM_PARTITIONS = 17
conf = SparkConf().setAppName("Alekseyen Spark01").setMaster("yarn")  # yarn # local[3]
sc = SparkContext(conf=conf)

edges = sc.textFile("/data/twitter/twitter_sample.txt")\
    .map(parse_edge)\
    .map(lambda e: (e[1], e[0])).partitionBy(NUM_PARTITIONS).cache()


start_vertex, end_vertex = 12, 34
paths = sc.parallelize([(start_vertex, str(start_vertex))]).partitionBy(NUM_PARTITIONS)

for _ in range(int(1e3)):
    paths = paths.join(edges).map(step)
    if paths.filter(lambda x: x[0] == end_vertex).count() > 0:
        break

_, path = paths.filter(lambda x: x[0] == end_vertex).first()
print(path)
