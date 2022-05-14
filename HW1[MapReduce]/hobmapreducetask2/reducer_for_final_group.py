#! /usr/bin/env python3
from collections import defaultdict
import sys
from operator import itemgetter

date_err_warn_counter = defaultdict(lambda: [0, 0])
for line in sys.stdin:
    date, errors_nums, warn_nums = line.split('\t')

    errors_nums, warn_nums = int(errors_nums), int(warn_nums)

    date_err_warn_counter[date][0] += errors_nums
    date_err_warn_counter[date][1] += warn_nums

# sort desc (errors, warns) and ascending date
# sorted_counter = sorted(date_err_warn_counter.items(), key=lambda item: (item[1], item[0]), reverse=True)
sorted_counter = sorted(sorted(date_err_warn_counter.items(), key=itemgetter(0)), key=itemgetter(1), reverse=True)

for date, (errors_num, warn_num) in sorted_counter:
    print(date, errors_num, warn_num, sep='\t')
