#! /usr/bin/env bash

TEXT_DIR="/data/minecraft-server-logs/*"
TMP_DIR="tmp_dir"
OUT_DIR="results"

NUM_REDUCERS=8

hadoop fs -rm -r -skipTrash $TMP_DIR >/dev/null
hadoop fs -rm -r -skipTrash $OUT_DIR >/dev/null

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapred.job.name="Alekseyen [120]" \
    -D mapreduce.job.reduces=${NUM_REDUCERS} \
    -D mapreduce.partition.keycomparator.options='-k1' \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -files mapper.py,reducer.py \
    -mapper "python3 mapper.py" \
    -reducer "python3 reducer.py" \
    -input ${TEXT_DIR} \
    -output $TMP_DIR >/dev/null

#hdfs dfs -cat ${TMP_DIR}/* | sort -k1 1>&2 # just for debugging

yarn jar /opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/hadoop-streaming.jar \
    -D mapreduce.job.name="group and sort output [120]" \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.partition.keycomparator.options='-k1' \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -files reducer_for_final_group.py \
    -mapper cat \
    -reducer "python3 reducer_for_final_group.py" \
    -input $TMP_DIR \
    -output $OUT_DIR >/dev/null

hdfs dfs -cat ${OUT_DIR}/part-00000 2>>/dev/null | head -10
