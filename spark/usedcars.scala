// Vars
val executors = sc.getConf.getInt("spark.executor.instances", 1)
val corePerExecutor = sc.getConf.getInt("spark.executor,cores", 1)
val partitions = 4 * executors * corePerExecutor

val replicationFactor = 100

// Read DF from HDFS
val rawDF = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("dataset/vehicles.csv")

// 1.Preprocessing
val cleanDF = rawDF.select("region", "price", "manufacturer", "fuel", "odometer").withColumnRenamed("manufacturer", "brand").na.drop

val preprocessedDF = cleanDF.withColumn("dummy", explode(array((0 until replicationFactor).map(lit): _*))).selectExpr(cleanDF.columns: _*).repartition(partitions)
preprocessedDF.cache

// 2a.Opi
val df2a = preprocessedDF.withColumn("opi", $"odometer" / $"price").select("brand", "opi").groupBy("brand").avg().withColumnRenamed("avg(opi)", "opi")

// 2b.Region
val brandCount = preprocessedDF.select("region", "brand").groupBy("region", "brand").count
brandCount.cache
val brandMax = brandCount.groupBy("region").max("count")
val df2b = brandMax.withColumn("count", col(brandMax.columns(1))).join(broadcast(brandCount), Seq("region", "count")).filter($"count" === $"max(count)").groupBy("region").agg(first("brand").as("brand"))

// 3.Join
val res = df2a.join(broadcast(df2b), Seq("brand")).select("region", "brand", "opi")
//res.collect

// Write results on HDFS
//res.write.parquet("spark-out")
res.write.csv("spark-out")
