#!/bin/bash

hdfs dfs -test -e spark-out
res=$?
if [[ $res == 0 ]]
then
  hdfs dfs -rm -r -skipTrash spark-out
fi 

spark2-shell -i usedcars.scala --num-executors=$1 --executor-cores=$2
