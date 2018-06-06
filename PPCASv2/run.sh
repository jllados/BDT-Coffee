#!/bin/bash

if [ ! -f src/PPCAS.so ]; then
    echo "Shared Library don't exist, run 'make' first."
    exit 1
fi

#spark-submit --packages datastax:spark-cassandra-connector:2.0.3-s_2.11 --conf spark.cassandra.connection.host=${cassandra_ip} --files src/PPCAS.so  --num-executors 80 --executor-cores 1 --master spark://${master_ip}:7077 src/PPCAS.py $1 $2 $3 $4 $5
spark-submit --packages datastax:spark-cassandra-connector:2.0.3-s_2.11 --conf spark.cassandra.connection.host=${cassandra_ip} --files src/PPCAS.so  --num-executors 80 --executor-cores 1 --master yarn src/PPCAS.py $1 $2 $3 $4 $5
#--driver-memory 1g --executor-memory 1g