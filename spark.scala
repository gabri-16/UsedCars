val rawDF = spark.read.format("csv").option("sep", ",").option("inferSchema", "true").option("header", "true").load("dataset/vehicles.csv")

val preprocessedDF = rawDF.select("region", "price", "manufacturer", "fuel", "odometer").withColumnRenamed("manufacturer", "brand").na.drop

preprocessedDF.cache

val df2a = preprocessedDF.withColumn("opi", $"odometer" / $"price").select("brand", "opi").groupBy("brand").avg().withColumnRenamed("avg(opi)", "opi")

val df2b = preprocessedDF.select("region", "brand").groupBy("region", "brand").count.groupBy("region").agg(max("count"), first("brand").as("brand")).select("region", "brand")

val res = df2a.join(df2b, Seq("brand")).select("region", "brand", "opi")
//res.collect

res.write.parquet("spark-out")
