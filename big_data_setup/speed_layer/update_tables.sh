#!/bin/bash
# update_tables.sh
beeline -u 'jdbc:hive2://10.0.0.50:10001/;transportMode=http' -f /home/sshuser/kritikarana/project/hive_queries/update_all_tables.hql >> /home/sshuser/kritikarana/project/speed_layer/update_tables.log 2>&1
