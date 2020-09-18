#!/bin/bash
# Launch it from project root to build the project and run the jobs

#Vars
n_reducer=20
combiner=0 # 0 -> off; any -> on

#Remove jar from hdfs (if present)
hdfs dfs -test -e uc.jar
res=$?
if [[ $res == 0  ]]
then
  hdfs dfs -rm -skipTrash uc.jar
fi

set -e # Break pipeline on error (don't move before test!)

#Compile src
javac -d bin -cp bin:$(hadoop classpath) src/utils/Car.java
javac -d bin -cp bin:$(hadoop classpath) src/utils/JoinPair.java
javac -d bin -cp bin:$(hadoop classpath) src/utils/BrandQuantityPair.java
javac -d bin -cp bin:$(hadoop classpath) src/utils/OpiAveragePair.java 
javac -d bin -cp bin:$(hadoop classpath) src/Preprocessing.java
javac -d bin -cp bin:$(hadoop classpath) src/Opi.java
javac -d bin -cp bin:$(hadoop classpath) src/Region.java
javac -d bin -cp bin:$(hadoop classpath) src/Join.java
javac -d bin -cp bin:$(hadoop classpath) src/combiner/OpiCombiner.java

# Create jar and put it in hdfs
cd bin
jar -cf uc.jar *
hdfs dfs -put uc.jar

# Launch jobs
start=$(date +%s)

hadoop jar uc.jar Preprocessing dataset/vehicles.csv out $n_reducer
if [[ $combiner == 0 ]]
then
  hadoop jar uc.jar Opi out out2a $n_reducer
else
  hadoop jar uc.jar OpiCombiner out out2a $n_reducer  
fi

hadoop jar uc.jar Region out out2b $n_reducer
hadoop jar uc.jar Join out2a out2b out3 $n_reducer

stop=$(date +%s)

# Show reults
hdfs dfs -ls out3
echo "Total time $(($stop - $start)) s"
#hdfs dfs -cat out3/part-r-00002

# Return to project root
cd ..
