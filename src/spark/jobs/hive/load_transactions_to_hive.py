from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, broadcast
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
transaction_path = f"{base_path}/situation_date={latest}"
fraud_path = f"/raw/fraud_labels/situation_date={latest}"

df = spark.read \
  .parquet(transaction_path)
  
error_df = df.select("id", "errors")
error_df = error_df.filter( col("errors").isNotNull() & (col("errors") != ""))

df = df.withColumn(
  "currency", regexp_extract(col("amount"), r"^(\D+)", 1)
).withColumn(
  "amount", regexp_replace(col("amount"), r"[^0-9\.-]", "").cast("double")
)

fldf = spark.read.csv(fraud_path, header=True, inferSchema=True)
fldf = fldf.withColumnRenamed("id", "transaction_id")

df = df.join(broadcast(fldf), df.id == fldf.transaction_id)

df.drop("transaction_id", "target", "errors")

df.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.transactions")   
  
error_df.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.errors")   

spark.stop()
