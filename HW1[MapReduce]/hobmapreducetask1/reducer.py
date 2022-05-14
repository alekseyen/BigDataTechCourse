#! /usr/bin/env python3

import sys

current_key = None
word_sum = 0

for line in sys.stdin:
    try:
        word, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue

    if current_key != word:
        if current_key and word_sum > 0:
            print(current_key.lower(), word_sum, sep='\t')
        word_sum = 0
        current_key = word

    word_sum += count if count > 0 else -float('inf')

if current_key and word_sum > 0:  # do not forget print final word
    print(current_key.lower(), word_sum, sep='\t')
