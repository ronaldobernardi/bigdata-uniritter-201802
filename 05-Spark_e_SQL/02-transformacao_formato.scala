val nyc_jan = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("s3a://nyc-tlc/trip data/yellow_tripdata_2017-01.csv")
val df = nyc_jan.summary()

df.show()

df.explain()

nyc_jan.printSchema()


df.write.parquet( "class_yellow_tripdata_2017-01.parquet" )
