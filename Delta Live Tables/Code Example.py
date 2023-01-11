# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

inputData = "Sales_January_2019"
dataPath = f"/mnt/adlslirkov/raw/{inputData}.csv"


# COMMAND ----------

config = spark.table("DLTConfig.sources").where(col("TableName") == inputData).first()
readoptions = json.loads(config.ReadOptions)
expectconf = json.loads(config.Expectations)

print(f"ReadOptions: {readoptions}")
print(f"Expectations: {expectconf}")

# COMMAND ----------

df = (spark.read
      .format("csv")
      .options(**options)
      .load(dataPath)
     )

df.display()

# COMMAND ----------

import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

inputData = "January_2019"
dataPath = f"/mnt/adlslirkov/raw/Sales_{inputData}.csv"

@dlt.table(
  comment="The raw wikipedia clickstream dataset, ingested from /databricks-datasets."
)
@dlt.expect_all(expectations)

def clickstream_raw():
    return (df)
