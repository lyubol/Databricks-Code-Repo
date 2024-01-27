# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# bronzeSourcePath = spark.conf.get("bronzeSourcePath")
# bronzeTableDestinationPath = spark.conf.get("bronzeTableDestinationPath")
# bronzeCheckpointPath = spark.conf.get("bronzeCheckpointPath")

bronzeCheckpointPath = "/mnt/adlslirkov/raw/SalesEndToEnd/Bronze/"
bronzeSourcePath = "/mnt/adlslirkov/raw/AutoLoaderInput/"
bronzeDeltaTablePath = "/mnt/adlslirkov/raw/SalesEndToEnd/Bronze/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Delta tables

# COMMAND ----------

# MAGIC %sql
# MAGIC USE Sales; 
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS Sales.SalesBronze
# MAGIC   (OrderId INT, Product STRING, QuantityOrdered INT, PriceEach DOUBLE, OrderDate STRING, PurchaseAddress STRING, LoadDate TIMESTAMP, SourceFileName STRING) 
# MAGIC LOCATION '/mnt/adlslirkov/raw/SalesEndToEnd/Bronze/';

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
    "cloudFiles.schemaEvolutionMode": "rescue",
    "cloudFiles.schemaLocation": "/mnt/adlslirkov/raw/SalesEndToEnd/Bronze/"
}

bronzeWriteConfig = {
    "checkpointLocation": bronzeCheckpointPath,
    "mergeSchema": True
}

bronzeSourceSchema = StructType([
    StructField("OrderId", IntegerType()),
    StructField("Product", StringType()),
    StructField("QuantityOrdered", IntegerType()),
    StructField("PriceEach", DoubleType()),
    StructField("OrderDate", StringType()),
    StructField("PurchaseAddress", StringType()),
    StructField("LoadDate", TimestampType()),
    StructField("SourceFileName", StringType())
])

(spark.readStream
 .format("cloudFiles")
 .options(**bronzeReadConfig)
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

# %sql
# SELECT * FROM Sales.SalesBronze

# COMMAND ----------


