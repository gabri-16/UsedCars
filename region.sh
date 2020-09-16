#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Region.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar Region out out3
hdfs dfs -ls out3
cd ..
hdfs dfs -cat out3/part-r-00002
