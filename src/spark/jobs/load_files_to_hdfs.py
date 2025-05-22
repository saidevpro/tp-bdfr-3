from pyspark.sql import SparkSession, functions as F

spark = (
  SparkSession.builder
  .appName("Read Data From FileSystem Write in HDFS")
  .enableHiveSupport()
  .getOrCreate()
)

hdfs_output_path = "hdfs://namenode:9000/raw"
num_partitions = 200

cards_data_path = '/data/cards_data.csv'
df = (
  spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(cards_data_path))

df = df.withColumn("situation_date", F.current_date())
df.write \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .parquet(f"{hdfs_output_path}/cards_data")
  
users_data_path = '/data/users_data.csv'
udf = (
  spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(users_data_path))

udf = udf.withColumn("situation_date", F.current_date())
udf.write \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .parquet(f"{hdfs_output_path}/users_data")
  
  
mcc_codes_path = '/data/mcc_codes.json'
mdf = (
  spark.read
  .option("multiLine", "true")
  .json(mcc_codes_path))

mdf = mdf.withColumn("situation_date", F.current_date())
mdf.write \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .parquet(f"{hdfs_output_path}/mcc_codes")
  
  
spark.stop()