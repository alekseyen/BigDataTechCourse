#! /usr/bin/env python3
import sys
import re

FILTER_REG = "[^A-Za-z\\s]"
APPROPRIATE_WORD_LEN = 6, 9

for line in sys.stdin:
    try:
        article_id, text = line.strip().split('\t', 1)
    except ValueError as e:
        continue

    for text_part in text.split():
        for word in re.split(FILTER_REG, text_part):
            if APPROPRIATE_WORD_LEN[0] <= len(word) <= APPROPRIATE_WORD_LEN[1]:
                if word[0].isupper() and all(map(lambda x: x.islower(), word[1:])):
                    print(word, 1, sep='\t')
                else:
                    # -2 special sign for reducer to show that we deal with deprecated word
                    print(word, -2, sep='\t')
