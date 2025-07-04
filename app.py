from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("IcebergExample") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .getOrCreate()

# Jadval yaratish va ma'lumot yozish
data = [("person1", 28, "Doctor"), ("person2", 35, "Singer"), ("person3", 42, "Teacher")]
columns = ["name", "age", "job_title"]
df = spark.createDataFrame(data, columns)

spark.sql("CREATE DATABASE IF NOT EXISTS db")
df.write.format("iceberg").mode("overwrite").saveAsTable("db.persons")

# Jadvalni o'qish
iceberg_df = spark.read.format("iceberg").load("db.persons")

print(iceberg_df.show())

