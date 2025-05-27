
from pyspark.sql import SparkSession, functions as F

TABLE_TRANSACTION = "default.transactions"
postgres_url = "jdbc:postgresql://datamart-db:5432/datamart"
postgres_properties = {
  "user": "root",
  "password": "root",
  "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
  .appName("Create Transactions to Datamart") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()
  
df = spark.sql(f"SELECT * FROM {TABLE_TRANSACTION}")

df = df.drop("year", "month")

df.write \
  .jdbc(
    url=postgres_url,
    table="transactions",
    mode="overwrite",
    properties=postgres_properties
)

spark.stop()

