from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col
import re

spark = SparkSession.builder \
  .appName("Loads transactions to hive") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()

base_path = "/raw/transactions"

hadoop_conf = spark._jsc.hadoopConfiguration()
fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
status = fs.listStatus(spark._jvm.org.apache.hadoop.fs.Path(base_path))

pattern = re.compile(r"situation_date=(\d{4}-\d{2}-\d{2})")
dates = []
for fileStatus in status:
  name = fileStatus.getPath().getName()
  m = pattern.match(name)
  if m:
    dates.append(m.group(1))

if not dates:
  raise RuntimeError(f"No partitions found under {base_path}")

latest = max(dates)
latest_path = f"{base_path}/situation_date={latest}"

df = spark.read.parquet(latest_path)

df = df.withColumn(
  "currency", regexp_extract(col("amount"), r"^(\D+)", 1)
).withColumn(
  "amount", regexp_replace(col("amount"), r"[^0-9\.-]", "").cast("double")
)

df.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.transactions")   

spark.stop()
