#! /usr/bin/env bash

hive -f create_tabel_in_several_formats.hql
hive -f get_table.hql
