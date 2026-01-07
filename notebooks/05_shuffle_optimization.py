from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# =========================
# 1. Enable AQE
# =========================
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# =========================
# 2. Tune shuffle partitions
# =========================
spark.conf.set("spark.sql.shuffle.partitions", "200")

df = spark.read.parquet("/Volumes/databricks_project_poc/filestore/dwd/cart_items")

# =========================
# 3. Repartition by key
# =========================
df_repartitioned = df.repartition("userId")

# =========================
# 4. Cache reused data
# =========================
df_repartitioned.cache()
df_repartitioned.count()

df_repartitioned.groupBy("userId").sum("total").show()
