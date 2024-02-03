# Databricks notebook source
# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Obtain parameters
entityName = spark.conf.get("entityName")
rawFilePath = spark.conf.get("rawFilePath")

# entityName = 'Sales'

# COMMAND ----------

# DBTITLE 1,Obtain entity metadata
entityConfig = spark.read.table("Metadata.EntityMetadata").where(col("EntityName") == entityName).collect()[0]
configFormat = entityConfig.EntityFormat
configOptions = entityConfig.EntityReadOptions
configExpectations = entityConfig.EntityExpectations
configDescription = entityConfig.EntityDescription

print(f"""
    Entity Metadata:
    ----------------
    EntityName: {entityName};
    EntityFormat: {configFormat};
    EntityReadOptions: {configOptions};
    EntityExpectations: {configExpectations};
    EntityDescription: {configDescription}.
""")

# COMMAND ----------

# DBTITLE 1,Read dataset
dfRaw = (spark.read
    .format()
    .options(**configOptions)
    .load(rawFilePath)
)

# COMMAND ----------

# DBTITLE 1,Apply transformations
dfRaw = (dfRaw
    .withColumn("LoadDate", current_timestamp())
    .withColumn("SourceFileName", input_file_name())
)

# COMMAND ----------

# DBTITLE 1,DLT logic
@dlt.table(
    comment = configComment,
    name = entityName
)
@dlt.expect_all(configExpectations)
def raw():
    return dfRaw
