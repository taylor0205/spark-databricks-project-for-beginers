# identify data skew
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("/Volumes/databricks_project_poc/filestore/dwd/cart_items")

# =========================
# 1. Check key distribution
# =========================
df.groupBy("userId").agg(count("*").alias("cnt")) \
  .orderBy("cnt", ascending=False) \
  .show(10)

# =========================
# 2. Trigger shuffle intentionally
# =========================
df.groupBy("userId").sum("total").explain(True)

df.groupBy("userId").sum("total").show()

# =========================
# 3. Notes (manual)
# =========================
# Check Spark UI:
# - Long-running tasks
# - Uneven shuffle read/write
