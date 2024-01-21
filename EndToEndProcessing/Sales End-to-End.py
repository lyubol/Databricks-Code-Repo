# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronzeSourcePath = spark.conf.get("bronzeSourcePath")
bronzeTableDestinationPath = spark.conf.get("bronzeTableDestinationPath")
bronzeCheckpointPath = spark.conf.get("bronzeCheckpointPath")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS Sales.SalesBronze
# MAGIC   (OrderId INT, Product STRING, QuantityOrdered INT, PriceEach DOUBLE, OrderDate STRING, PurchaseAddress STRING, LoadDate TIMESTAMP, SourceFileName STRING) 
# MAGIC LOCATION '/mnt/adlslirkov/raw/SalesEndToEnd/'

# COMMAND ----------

# %sql
# CREATE TABLE IF NOT EXISTS Sales.SalesSilver
#   (OrderId INT, Product STRING, QuantityOrdered INT, PriceEach DOUBLE, OrderDate DATE, PurchaseAddress STRING) 
# LOCATION '/mnt/adlslirkov/raw/SalesEndToEnd/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest Data with Auto Loader

# COMMAND ----------

# DBTITLE 1,Append to Bronze
bronzeReadConfig = {
    "cloudFiles.format": "csv",
    "cloudFiles.inferColumnTypes": True,
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.schemaLocation": "/mnt/adlslirkov/raw/AutoLoaderOutput/Schema/"
}

bronzeWriteConfig = {
    "checkpointLocation": bronzeCheckpointPath,
    "mergeSchema": True
}

bronzeSourceSchema = StructType([
    StructField("OrderId", IntegerType()),
    StructField("Product", StringType()),
    StructField("QuantityOrdered", StringType()),
    StructField("PriceEach", DoubleType()),
    StructField("OrderDate", StringType()),
    StructField("PurchaseAddress", StringType()),
    StructField("LoadDate", TimestampType()),
    StructField("SourceFileName", StringType())
])

(spark.readStream
 .format("cloudFiles")
 .options(**bronzeConfig)
 .schema(bronzeSourceSchema)
 .load(bronzeSourcePath)
 .withColumn("LoadDate", current_timestamp())
 .withColumn("SourceFileName", input_file_name())
 .writeStream
 .options(**bronzeWriteConfig)
 .trigger(once=True)
 .table("Sales.SalesBronze")
)

# COMMAND ----------

# .withColumn("OrderDate", regexp_replace(col("OrderDate"), "2019", "19"))
# .withColumn("OrderDate", to_timestamp("OrderDate", "MM/dd/yy HH:mm"))
