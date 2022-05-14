#! /usr/bin/env python3
import sys
import re

FILTER_REG = re.compile(r".*\[(?P<date>\d\d\d\d\-\d\d\-\d\d)\.\d\d\:\d\d\:\d\d\]\s\[(?P<problem_type>.+\sthread.*)\]:\s")

for line in sys.stdin:
    filtred_str = FILTER_REG.match(line)
    if filtred_str is None:
        continue

    results = filtred_str.groupdict()

    if results["problem_type"] and ('WARN' in results["problem_type"] or 'ERROR' in results["problem_type"]):
        print(results["date"], 'ERROR' if 'ERROR' in results["problem_type"] else 'WARN', 1, sep='\t')
