#! /usr/bin/env bash

#hive -e "use podkidysheval; show tables" | xargs -I '{}' hive -e 'use podkidysheval; drop table {}';
hive -f kkt_creation.hql
