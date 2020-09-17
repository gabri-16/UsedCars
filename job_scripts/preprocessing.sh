#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Preprocessing.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar Preprocessing dataset/vehicles.csv out
hdfs dfs -ls out
cd ..
#hdfs dfs -cat out/part-r-00002
