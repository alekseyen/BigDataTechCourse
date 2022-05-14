#!/usr/bin/env python3

import sys
import io
import happybase

import random

input_stream = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
output_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

current_key = None
sum_count = 0

hosts = ['mipt-node01.atp-fivt.org', 'mipt-node02.atp-fivt.org']

host = random.choice(hosts)

connection = happybase.Connection(host)

table = connection.table('akhtyamovpavel_wordcount')
batch = table.batch()


global_cnt = 0

def get_round_digits(count):
    result = 1
    count //= 10
    while count > 0:
        result *= 10
        count //= 10
    return result


for line in input_stream:
    try:
        key, count = line.strip().split('\t', 1)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            # the -> 10^8 times
            #batch.put(
            #    "{}_{}".format(len(current_key), current_key).encode(), {b'count:count': '{}'.format(sum_count).encode()})
            
            #batch.put(
            #        current_key.encode(), {'count:{}'.format(get_round_digits(sum_count)).encode(): '{}'.format(sum_count).encode()})
            
            bigger_order = get_round_digits(sum_count)
            
            # bigger_order = 10^8
            
            encoded_key = current_key.encode()
            
            encoded_values = {}
            while bigger_order >= 1:
                encoded_values['count:ge_{}'.format(bigger_order).encode()] = str(sum_count).encode()
                bigger_order //= 10
            
            batch.put(
                encoded_key, encoded_values
            )
            
            # count:ge_10000000
            # count:ge_1000000
            # count:ge_100000
            # count:ge_10000
            global_cnt += 1
            if global_cnt % 10000 == 0:
                batch.send()
                batch = table.batch()
        sum_count = 0
        current_key = key
    sum_count += count


if current_key:
    batch.put("{}_{}".format(len(current_key), current_key).encode(), {b'count:count': '{}'.format(sum_count).encode()})
    batch.put(
       current_key.encode(), {
           'count:{}'.format(get_round_digits(sum_count)).encode(): '{}'.format(sum_count).encode()
       }
    )

batch.send()

