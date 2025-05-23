from pyspark.sql import SparkSession, functions as F
import re

spark = SparkSession.builder \
  .appName("Loads MCC codes to hive") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()

base_path = "/raw/users_data"

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
  "currency", F.regexp_extract(F.col("per_capita_income"), r"^(\D+)", 1)
).withColumn(
  "per_capita_income", F.regexp_replace(F.col("per_capita_income"), r"[^0-9\.-]", "").cast("double")
).withColumn(
  "yearly_income", F.regexp_replace(F.col("yearly_income"), r"[^0-9\.-]", "").cast("double")
).withColumn(
  "total_debt", F.regexp_replace(F.col("total_debt"), r"[^0-9\.-]", "").cast("double")
).drop(
  "latitude", "longitude"
)

df.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.users")   

spark.stop()
