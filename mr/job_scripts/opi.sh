#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Opi.java
javac -d bin -cp bin:$(hadoop classpath) src/combiner/OpiCombiner.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar OpiCombiner out out2a
hdfs dfs -ls out2a
cd ..
#hdfs dfs -cat out2a/part-r-00002
