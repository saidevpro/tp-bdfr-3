from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DateType
from pyspark.sql.functions import col

SOURCE_PATH_CSV         = 'hdfs://namenode:9000/raw/'
SOURCE_PATH_TXT        = 'hdfs://namenode:9000/raw/transactions'

spark = SparkSession.builder \
  .appName("Load transactions from HDFS -> HIVE") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/hive/warehouse") \
  .enableHiveSupport() \
  .getOrCreate()
  
spark.sql("CREATE DATABASE IF NOT EXISTS golden;")

df = spark.read \
  .format("parquet") \
  .option("header", "true") \
  .load(SOURCE_PATH_CSV)
  
df.show()  
  
df.write \
  .outputMode("append") \
  .toTable("golden.transactions")

spark.stop()
  
