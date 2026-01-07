import requests
import json
from pyspark.sql import SparkSession

def load_api():
    url = "https://dummyjson.com/carts"
    response = requests.get(url)
    data = response.json()["carts"]
    spark = SparkSession.builder.appName("Spark API").getOrCreate()
    df_api = spark.createDataFrame(data,schema=None)
    return df_api

# ans = load_api()
# display(ans)

def load_csv():
    df_product = (
        spark.read
        .option("header", "true")
        .csv("/Volumes/databricks_project_poc/filestore/data//products.csv")
    )
    return df_product

# df_product.show()

# 3. Persist ODS layer

df_carts = load_api()
df_carts.write.mode("overwrite").mode("overwrite").parquet("/Volumes/databricks_project_poc/filestore/ods/carts")

df_products = load_csv()
df_products.write.mode("overwrite").parquet("/Volumes/databricks_project_poc/filestore/ods/products")
