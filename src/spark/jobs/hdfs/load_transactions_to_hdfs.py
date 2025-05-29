from pyspark.sql import SparkSession, functions as F

spark = (
  SparkSession.builder
  .appName("Read transactions data From BD Write it in HDFS")
  .enableHiveSupport()
  .getOrCreate()
)

jdbc_hostname = "mariadb"
jdbc_port     = 3306
jdbc_database = "bdd"
jdbc_url      = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"
connection_props = {
  "user": "root",
  "password": "root",
  "driver": "com.mysql.cj.jdbc.Driver"
}

target_hive_table = "warehouse.transactions"

source_table = "transactions"
bounds = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", f"(SELECT MIN(id) AS lo, MAX(id) AS hi FROM {source_table}) t") \
  .options(**connection_props) \
  .load() \
  .collect()[0]
  
lower, upper = bounds["lo"], bounds["hi"]

num_partitions = 8
df = spark.read \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("dbtable", source_table) \
  .option("partitionColumn", "id") \
  .option("lowerBound", lower) \
  .option("upperBound", upper) \
  .option("numPartitions", num_partitions) \
  .option("fetchsize", 10_000) \
  .options(**connection_props) \
  .load()

hdfs_output_path = "hdfs://namenode:9000/raw/transactions"

df = (
  df
    .withColumn("year",  F.year("situation_date"))
    .withColumn("month", F.month("situation_date"))
)

df = df.repartition("situation_date", "year", "month")

df.write \
  .mode("append") \
  .partitionBy("situation_date", "year", "month") \
  .parquet(hdfs_output_path)

spark.stop()