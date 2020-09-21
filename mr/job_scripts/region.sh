#!/bin/bash
set -e

javac -d bin -cp bin:$(hadoop classpath) src/Region.java
javac -d bin -cp bin:$(hadoop classpath) src/combiner/RegionCombiner.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm -skipTrash uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar RegionCombiner out out2b
hdfs dfs -ls out2b
cd ..
#hdfs dfs -cat out2b/part-r-00002
