# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Set file name and path
inputData = spark.conf.get("pipeline.entityName")
dataPath = f"/mnt/adlslirkov/raw/{inputData}.csv"

# COMMAND ----------

config = spark.table("DLTConfig.sources").where(col("TableName") == inputData).first()
readoptions = json.loads(config.ReadOptions)
expectconf = json.loads(config.Expectations)

print(f"ReadOptions: {readoptions}")
print(f"Expectations: {expectconf}")

# COMMAND ----------

# Read
df = (spark.read
      .format("csv")
      .options(**readoptions)
      .load(dataPath)
     )

# Transform
df = df.toDF(*[col.replace(" ", "") for col in df.columns])

df = df.withColumn("OrderDate", to_timestamp("OrderDate", "MM/dd/yy HH:mm"))

df.display()

# COMMAND ----------

import dlt

@dlt.table(
    name=f"DLT{inputData}",
    comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)

@dlt.expect_all(expectconf)
def clickstream_raw():
    return (df)
