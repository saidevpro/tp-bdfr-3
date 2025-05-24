
from pyspark.sql import SparkSession, functions as F

TABLE_TRANSACTION = "default.transactions"
postgres_url = "jdbc:postgresql://datamart-db:5432/datamart"
postgres_properties = {
  "user": "root",
  "password": "root",
  "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
  .appName("Create Transactions Metrics to Datamart") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()
  
df = spark.sql(f"SELECT * FROM {TABLE_TRANSACTION}")

agg_amount_df = spark.sql("""
  SELECT
    YEAR(date) AS year,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount),2) AS total_amount,
    ROUND(AVG(amount),2) AS avg_amount,
    ROUND(100 * AVG(CASE WHEN has_fraud = 'Yes' THEN 1.0 ELSE 0.0 END), 2) AS pct_fraud
  FROM transactions
  GROUP BY YEAR(date)
  ORDER BY YEAR(date)
""")

agg_amount_df.write \
  .jdbc(
    url=postgres_url,
    table="transactions_yearly_metrics",
    mode="overwrite",
    properties=postgres_properties
)
  
use_chip_df = df.withColumn(
  "fraud_flag",
  F.when(F.col("has_fraud") == "Yes", 1).otherwise(0)
).groupBy("use_chip") \
   .agg(
     F.count("*").alias("total_transactions"),
     F.round(100*F.avg("fraud_flag"),2).alias("fraud_pct"),
     F.round(F.avg("amount"),2).alias("avg_amount")
  )
   
use_chip_df.write \
  .jdbc(
    url=postgres_url,
    table="use_chip_metrics",
    mode="overwrite",
    properties=postgres_properties
)

spark.stop()

