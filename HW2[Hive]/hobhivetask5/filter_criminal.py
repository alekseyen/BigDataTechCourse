#!/usr/bin/env python3

import sys

prev_iin, prev_kkt_id = '', ''
is_criminal = False

for line in sys.stdin:
    line = line.strip()
    current_iin, current_kkt_id, subtype = line.split('\t')

    if current_iin != prev_iin:
        is_criminal = False
        prev_iin = current_iin

    if current_kkt_id != prev_kkt_id:
        is_criminal = False
        prev_kkt_id = current_kkt_id

    if subtype == 'receipt' and (subtype == 'openShift') == is_criminal == False:
        is_criminal = True
        print(current_iin)
