
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window


TABLE_USER = "default.users"
postgres_url = "jdbc:postgresql://datamart-db:5432/datamart"
postgres_properties = {
  "user": "root",
  "password": "root",
  "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
  .appName("Create Users Metrics to Datamart") \
  .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
  .config("spark.sql.catalogImplementation", "hive") \
  .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
  .enableHiveSupport() \
  .getOrCreate()

df = spark.sql(f"SELECT * FROM {TABLE_USER}")

gdf = (
  df.groupBy("gender")
    .agg(
      F.round(F.avg("total_debt"), 2).alias("avg_total_debt"),
      F.round(F.avg("credit_score"), 2).alias("avg_credit_score"),
      F.round(F.avg("yearly_income"), 2).alias("avg_yearly_income")
    )
)

gdf.write \
  .jdbc(
    url=postgres_url,
    table="gender_metrics",
    mode="overwrite",
    properties=postgres_properties
)

dti = df.withColumn(
  "debt_to_income",
  F.expr("total_debt / yearly_income")
).selectExpr(
  "round(avg(debt_to_income),2) as avg_dti",
  "round(max(debt_to_income), 2) as max_dti"
)


dti.write \
  .jdbc(
    url=postgres_url,
    table="dti_metrics",
    mode="overwrite",
    properties=postgres_properties
)

df_age = df.withColumn("age_bucket", F.floor(F.col("current_age")/10)*10)
df_age = df_age.groupBy("age_bucket") \
  .agg(
    F.round(F.avg("yearly_income"),2).alias("avg_income"),
    F.round(F.avg("total_debt"),2).alias("avg_debt"),
    F.round(F.avg("credit_score"),2).alias("avg_credit_score"),
    F.count("*").alias("total")
  ) \
  .orderBy("age_bucket")


df_age.write \
  .jdbc(
    url=postgres_url,
    table="age_bucket_metrics",
    mode="overwrite",
    properties=postgres_properties
)
  
df_scores = df.withColumn("score_band",
  F.when(F.col("credit_score") < 580, "Poor")
 .when(F.col("credit_score") < 670, "Fair")
 .when(F.col("credit_score") < 740, "Good")
 .otherwise("Excellent")
).groupBy("score_band") \
  .agg(
    F.round(F.avg("yearly_income"),2).alias("avg_income"),
    F.round(F.avg("total_debt"),2).alias("avg_debt"),
    F.count("*").alias("num_customers")
  )
  
df_scores.write \
  .jdbc(
    url=postgres_url,
    table="score_classification",
    mode="overwrite",
    properties=postgres_properties
)
  
df_rt = df.withColumn("years_to_retirement",
  F.col("retirement_age") - F.col("current_age")
).select(
  F.round(F.avg("years_to_retirement"),2).alias("avg_years_left"),
  F.min("years_to_retirement").alias("min_years_left"),
  F.max("years_to_retirement").alias("max_years_left")
)

df_rt.write \
  .jdbc(
    url=postgres_url,
    table="retirement_stats",
    mode="overwrite",
    properties=postgres_properties
)
  
tx  = spark.table("transactions")
users = spark.table("users")

totals = tx.groupBy("client_id").agg(F.sum("amount").alias("total_sent"))

w = Window.orderBy(F.desc("total_sent"))
ranked = totals.withColumn("rank", F.rank().over(w))

top10 = (ranked
  .filter("rank <= 10")
  .join(users, ranked.client_id == users.id, "inner")
  .select(
    users["*"],
    ranked["total_sent"],
    ranked["rank"]
  )
  .orderBy(F.desc("total_sent"))
)

top10.write \
  .jdbc(
    url=postgres_url,
    table="top_10_clients",
    mode="overwrite",
    properties=postgres_properties
)

spark.stop()

