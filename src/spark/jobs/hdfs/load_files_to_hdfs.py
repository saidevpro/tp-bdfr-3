from pyspark.sql import SparkSession, functions as F
from pyspark import SparkFiles

spark = (
  SparkSession.builder
  .appName("Read Data From FileSystem Write in HDFS")
  .enableHiveSupport()
  .getOrCreate()
)

hdfs_output_path = "hdfs://namenode:9000/raw"
num_partitions = 200

cards_data_path = f"file:///data/cards_data.csv"
df = (
  spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(cards_data_path))

df = df.withColumn("situation_date", F.current_date())
df.write \
  .option("header", "true") \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .csv(f"{hdfs_output_path}/cards_data")
  
users_data_path = f"file:///data/users_data.csv"
udf = (
  spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(users_data_path))

udf = udf.withColumn("situation_date", F.current_date())

udf.write \
  .option("header", "true") \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .csv(f"{hdfs_output_path}/users_data")
  
mcc_codes_path = f"file:///data/mcc_codes.json"
mdf = (
  spark.read
  .option("multiLine", "true")
  .json(mcc_codes_path))

mdf = mdf.withColumn("situation_date", F.current_date())
mdf.write \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .json(f"{hdfs_output_path}/mcc_codes")
  

fraud_data_path = f"file:///data/train_fraud_labels.csv"
fdf = (
  spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(fraud_data_path))

fdf = fdf.withColumn("situation_date", F.current_date())
fdf.write \
  .option("header", "true") \
  .mode("overwrite") \
  .partitionBy("situation_date") \
  .csv(f"{hdfs_output_path}/fraud_labels")
  
spark.stop()