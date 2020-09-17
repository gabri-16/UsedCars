#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Join.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar Join out2 out3 out4
hdfs dfs -ls out4
cd ..
hdfs dfs -cat out4/part-r-00002
