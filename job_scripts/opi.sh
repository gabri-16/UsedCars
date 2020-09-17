#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Opi.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar Opi out out2
hdfs dfs -ls out2
cd ..
#hdfs dfs -cat out2/part-r-00002
