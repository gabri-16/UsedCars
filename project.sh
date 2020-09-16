#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Project.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar Project car-test.csv out
hdfs dfs -ls out
cd ..
hdfs dfs -cat out/part-r-00002
