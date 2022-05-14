#!/usr/bin/env bash

PYSPARK_DRIVER_PYTHON=/usr/bin/python3 PYSPARK_PYTHON=/usr/bin/python3 spark2-submit \
  --conf spark.ui.port=5555 find_shortes_way_df.py
