from pyspark.sql import SparkSession, functions as F
import re

spark = SparkSession.builder \
  .appName("Loads MCC codes to hive") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()

base_path = "/raw/cards_data"

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
path = f"{base_path}/situation_date={latest}"

df = spark.read.csv(path, header=True, inferSchema=True)

df = df.withColumn(
  "card_number",
  F.regexp_replace(F.col("card_number"), r".(?=.{3})", "*")
).withColumn(
  "credit_limit_currency", F.regexp_extract(F.col("credit_limit"), r"^(\D+)", 1)
).withColumn(
  "credit_limit", F.regexp_replace(F.col("credit_limit"), r"[^0-9\.-]", "").cast("double")
)

df = df.drop("cvv")

df.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.cards")   

spark.stop()
