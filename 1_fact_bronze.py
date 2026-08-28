# Databricks notebook source
# MAGIC %md
# MAGIC ### Bronze Layer

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
catalog_name = 'portfolio'

# COMMAND ----------

# MAGIC %md
# MAGIC ### PSX Current

# COMMAND ----------

# SYMBOL,SECTOR,LISTED IN,LDCP,OPEN,HIGH,LOW,CURRENT,CHANGE,CHANGE (%),VOLUME

schema = StructType([
    StructField('Ticker', StringType(), True),
    StructField('Sector', StringType(), True),
    StructField('Listed In', StringType(), True),
    StructField('LDCP', StringType(), True),
    StructField('Open', StringType(), True),
    StructField('High', StringType(), True),
    StructField('Low', StringType(), True),
    StructField('Current', StringType(), True),
    StructField('Change', StringType(), True),
    StructField('Change (%)', StringType(), True),
    StructField('Volume', StringType(), True),
    
])

# COMMAND ----------

# folder path
folder_path = "/Volumes/portfolio/source/raw_s3/lambda_psx_csv"
files = [f for f in dbutils.fs.ls(folder_path) if f.name.endswith(".csv")]

if not files:
    raise FileNotFoundError(f"No CSV files found in {folder_path}")

newest_file_info = max(files, key=lambda f: f.modificationTime)
latest_file_path = newest_file_info.path

# Read the latest file
df = spark.read.option('header', 'true').option('delimiter', ',').option("quote", "\"").schema(schema).csv(latest_file_path) \
    .withColumn('ingest_timestamp', F.current_timestamp())


# COMMAND ----------

df = df.withColumnRenamed('Listed In', 'Listed_In').withColumnRenamed('Change (%)', 'Change_%')

# COMMAND ----------

df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog_name}.bronze.brz_psx_current")