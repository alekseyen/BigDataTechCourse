#!/usr/bin/env python3

import sys
import io
import re

import happybase

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
output_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


pattern = re.compile('\W*(\w+)\W*')

for line in input_stream:
    try:
        article_id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue
    words = text.split()
    for word in words:
        matcher = pattern.search(word)
        if matcher is not None:
            perfect_word = matcher.group(1).lower()
            print("{}\t1".format(perfect_word), file=output_stream)
