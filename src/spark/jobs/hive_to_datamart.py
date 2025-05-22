from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, regexp_replace, col, to_date, count, when

# Init Spark + Hive
spark = SparkSession.builder \
    .appName("Hive to DataMart - Transactions") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .enableHiveSupport() \
    .getOrCreate()

# Lire la table Hive
df = spark.table("default.transactions")

# Nettoyer le montant
df = df.withColumn(
    "currency", regexp_extract(col("amount"), r"^(\D+)", 1)
).withColumn(
    "amount", regexp_replace(col("amount"), r"[^0-9\.-]", "").cast("double")
)

# ============================
# MÉTRIQUES ANALYTIQUES
# ============================

# 1. Nombre total de transactions
total_transactions = df.count()
print("✅ Nombre total de transactions :", total_transactions)

# 2. Montant total et moyen par devise
print("✅ Montant total et moyen par devise :")
df.groupBy("currency").agg(
    {"amount": "sum", "amount": "avg"}
).withColumnRenamed("sum(amount)", "total_amount") \
 .withColumnRenamed("avg(amount)", "average_amount") \
 .show()

# 3. Statistiques descriptives globales
print("✅ Statistiques descriptives globales :")
df.select("amount").describe().show()

# 4. Top 5 des transactions les plus élevées
print("✅ Top 5 transactions les plus élevées :")
df.orderBy(col("amount").desc()).show(5)

# 5. Nombre de transactions par devise
print("✅ Nombre de transactions par devise :")
df.groupBy("currency").count().show()

# 6. Répartition par tranches de montants
print("✅ Histogramme simple par tranche de montants :")
df = df.withColumn("amount_range", when(col("amount") < 100, "<100")
                                     .when(col("amount") < 1000, "100-999")
                                     .when(col("amount") < 10000, "1000-9999")
                                     .otherwise(">=10000"))
df.groupBy("amount_range").count().orderBy("amount_range").show()

# 7. Transactions par date si colonne 'date' présente
if 'date' in df.columns:
    df = df.withColumn("transaction_date", to_date("date"))
    print("✅ Transactions par jour :")
    df.groupBy("transaction_date").agg(count("*").alias("nb_transactions")).show()

# ============================
# ÉCRITURE DANS POSTGRESQL
# ============================

# Config JDBC PostgreSQL
postgres_url = "jdbc:postgresql://datamart-db:5432/datamart"
postgres_properties = {
    "user": "root",
    "password": "root",
    "driver": "org.postgresql.Driver"
}

# Écriture des données transformées
df.write.jdbc(
    url=postgres_url,
    table="transactions",
    mode="overwrite",  # ou "append"
    properties=postgres_properties
)

# Terminer la session Spark
spark.stop()
