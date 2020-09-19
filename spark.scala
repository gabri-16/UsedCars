val rawDF = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("dataset/vehicles.csv")

val cleanDF = rawDF.select("region", "price", "manufacturer", "fuel", "odometer").withColumnRenamed("manufacturer", "brand").na.drop

val preprocessedDF = cleanDF.withColumn("dummy", explode(array((0 until 100).map(lit): _*))).selectExpr(cleanDF.columns: _*)

preprocessedDF.cache

val df2a = preprocessedDF.withColumn("opi", $"odometer" / $"price").select("brand", "opi").groupBy("brand").avg().withColumnRenamed("avg(opi)", "opi")

val brandCount = preprocessedDF.select("region", "brand").groupBy("region", "brand").count
val brandMax = brandCount.groupBy("region").max("count")
val df2b = brandMax.withColumn("count", col(brandMax.columns(1))).join(brandCount, Seq("region", "count")).filter($"count" === $"max(count)").groupBy("region").agg(first("brand").as("brand"))
//val df2b = preprocessedDF.filter($"fuel" === "gas").select("region", "brand").groupBy("region", "brand").count.groupBy("region").agg(max("count"), first("brand").as("brand")).select("region", "brand")

val res = df2a.join(df2b, Seq("brand")).select("region", "brand", "opi")
//res.collect

//res.write.parquet("spark-out")
res.write.csv("spark-out")
