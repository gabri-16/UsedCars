#!/bin/bash

hdfs dfs -test -e spark-out
res=$?
if [[ $res == 0  ]]
then
  hdfs dfs -rm -r -skipTrash spark-out
fi

spark2-shell -i $1 --num-executors $2 --executor-cores $3
