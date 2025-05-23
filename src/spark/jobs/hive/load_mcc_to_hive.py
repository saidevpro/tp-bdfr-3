from pyspark.sql import SparkSession, functions as F
import re

spark = SparkSession.builder \
  .appName("Loads MCC codes to hive") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()

base_path = "/raw/mcc_codes"

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


raw_mcc = spark.read.json(path, multiLine=True) 
cols = raw_mcc.columns

mcc = (
  raw_mcc.select(       
    F.explode(
      F.map_from_arrays(F.array([F.lit(int(c)) for c in cols]), F.array([F.col(c) for c in cols]))
    ).alias("mcc", "label")
  )
)

mcc.write \
  .format("parquet") \
  .mode("overwrite") \
  .saveAsTable("default.mcc")   

spark.stop()
