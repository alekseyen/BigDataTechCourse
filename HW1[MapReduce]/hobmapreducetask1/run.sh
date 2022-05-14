#! /usr/bin/env bash

TEXT_DIR="/data/wiki/en_articles"
TMP_DIR="tmp_dir"
OUT_DIR="results"

NUM_REDUCERS=8

hadoop fs -rm -r -skipTrash $TMP_DIR* >/dev/null
hadoop fs -rm -r -skipTrash $OUT_DIR* >/dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="Alekseyen [111]" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -combiner "python3 reducer.py" \
    -reducer "python3 reducer.py" \
    -input ${TEXT_DIR} \
    -output $TMP_DIR >/dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="sort_output" \
    -D stream.num.map.output.key.fields=2 \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapreduce.lib.partition.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options='-k2,2nr -k1' \
    -mapper cat \
    -reducer cat \
    -input $TMP_DIR \
    -output $OUT_DIR >/dev/null

hdfs dfs -cat ${OUT_DIR}/part-00000 2>>/dev/null | head -10
