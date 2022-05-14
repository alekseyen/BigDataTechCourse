#!/usr/bin/env bash

OUT_DIR="streaming_wc_result"
NUM_REDUCERS=8

hdfs dfs -rm -r -skipTrash $OUT_DIR*

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="akhtyamov_wordcount_example" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper mapper.py \
	-reducer reducer.py \
    -input /data/wiki/en_articles \
    -output $OUT_DIR

