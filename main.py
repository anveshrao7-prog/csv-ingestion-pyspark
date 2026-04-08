from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("CSV Ingestion Project") \
    .getOrCreate()

df = spark.read.option("header", True) \
    .option("inferSchema", True)\
    .csv("data/movies.csv")

#df.show(10,False)
df.printSchema()

df.write.mode("overWrite").parquet("output/data_parquet")
print("Task completed successfully!")