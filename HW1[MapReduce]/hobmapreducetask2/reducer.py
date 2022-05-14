#! /usr/bin/env python3

import sys

current_date = None
warnings_num, errors_num = 0, 0

for line in sys.stdin:
    date, problem_type, _ = line.split('\t')

    if current_date != date:
        if warnings_num + errors_num > 0:
            print(current_date, errors_num, warnings_num, sep='\t')

        warnings_num, errors_num = 0, 0
        current_date = date

    if str(problem_type) == "ERROR":
        errors_num += 1
    elif str(problem_type) == "WARN":
        warnings_num += 1
    else:
        raise ValueError(problem_type)

if warnings_num + errors_num > 0:  # do not forget print final problem_type
    print(current_date, errors_num, warnings_num, sep='\t')
