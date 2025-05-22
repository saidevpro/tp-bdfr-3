from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
  .appName("Load Fraud Labels to HDFS")
  .enableHiveSupport()
  .config("spark.sql.adaptive.enabled", "true")
  .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
  .getOrCreate()
)

hdfs_output_path = "hdfs://namenode:9000/raw"
fraud_labels_path = '/data/train_fraud_labels.json'

# Use batch read instead of streaming for static file
df = spark.read \
  .option("mode", "PERMISSIVE") \
  .json(fraud_labels_path)

# Write directly as parquet
df.coalesce(200) \
  .write \
  .mode("overwrite") \
  .option("compression", "snappy") \
  .parquet(f"{hdfs_output_path}/fraud_labels")

spark.stop()