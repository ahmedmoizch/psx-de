# Databricks notebook source
# MAGIC %md
# MAGIC ### Silver layer

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, DoubleType
catalog_name = 'portfolio'

# COMMAND ----------

df = spark.read.table(f"{catalog_name}.bronze.brz_psx_current")
# display(df.limit(5))

# COMMAND ----------

df = df.withColumn('Change_%', F.regexp_replace(F.col('Change_%'), '%', '').cast(FloatType()))
# df.show(5)

# COMMAND ----------

#df.printSchema()

# COMMAND ----------

cast_col = ['LDCP', 'Open', 'Current', 'High', 'Low', 'Change', 'Volume']
for c in cast_col:
    df = df.withColumn(c, df[c].cast(DoubleType()))

# COMMAND ----------

df = df.withColumn("Listed_In", F.split(F.trim(F.col("Listed_In")), r"\s*,\s*"))

# COMMAND ----------

# display(df.limit(5))

# COMMAND ----------

df_silver = df.withColumnRenamed("Ticker", "ticker") \
              .withColumnRenamed("Sector", "sector") \
              .withColumnRenamed("Listed_In", "listed_in") \
              .withColumnRenamed("LDCP", "ldcp") \
              .withColumnRenamed("Open", "open") \
              .withColumnRenamed("High", "high") \
              .withColumnRenamed("Low", "low") \
              .withColumnRenamed("Current", "current") \
              .withColumnRenamed("Change", "change") \
              .withColumnRenamed("Change_%", "change_percent") \
              .withColumnRenamed("Volume", "volume") \
              .withColumnRenamed("ingest_timestamp", "ingest_timestamp")
# display(df_silver.limit(5))


# COMMAND ----------

df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_psx_current")