# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.functions import col
import pyspark.sql.types

catalog_name = 'portfolio'

# COMMAND ----------

df = spark.read.table(f'{catalog_name}.silver.slv_psx_current')

# COMMAND ----------

df = df.withColumn(
    "date_id", 
    F.date_format(F.col("ingest_timestamp"), "yyyyMMdd").cast("int")
)

# COMMAND ----------

df = df.withColumn(
    "has_fault",
    (F.col("low") < 0).cast("integer")
)

# COMMAND ----------

df = df.withColumn('asset_class', F.lit('PSX'))

# COMMAND ----------

# Test Function
def validate_no_negative_prices(df):
    negative_count = df.filter(col("current") < 0).count()
    return negative_count

# Run validation
negative_count = validate_no_negative_prices(df)

# Test Condiditon
if negative_count == 0:
    print("All tests passed! Writing to Gold Delta Table...")

else:
    df = df.withColumn("has_fault", F.lit(1))


# COMMAND ----------

df.write.format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalog_name}.gold.gld_psx_current")