#!/usr/bin/env bash

spark2-submit  --conf spark.ui.port=5555 find_top_bigrams.py

