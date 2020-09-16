javac -d bin -cp bin:$(hadoop classpath) src/Region.java
cd bin
jar -cf uc.jar *
hdfs dfs -rm uc.jar
hdfs dfs -put uc.jar
hadoop jar uc.jar Region out out3
