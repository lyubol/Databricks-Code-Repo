# Databricks notebook source
# DBTITLE 1,Imports
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# COMMAND ----------

# DBTITLE 1,Obtain parameters
# entityName = spark.conf.get("entityName")
# rawFilePath = spark.conf.get("rawFilePath")

entityName = 'Sales'

# COMMAND ----------

# DBTITLE 1,Obtain entity metadata
entityConfig = spark.read.table("Metadata.EntityMetadata").where(col("EntityName") == entityName).first()
configEntityId = entityConfig.EntityId
configReadOptions = json.loads(entityConfig.ReadOptions)
configWriteOptions = json.loads(entityConfig.WriteOptions)
configSourceLocation = entityConfig.SourceLocation
configTargetTable = entityConfig.TargetTable
configExpectations = json.loads(entityConfig.Expectations)
configDescription = entityConfig.Description

print(f"""
    Entity Metadata:
    ----------------
    EntityId: {configEntityId}
    EntityName: {entityName};
    ReadOptions: {configReadOptions};
    WriteOptions: {configWriteOptions};
    SourceLocation: {configSourceLocation};
    TargetTable: {configTargetTable};
    Expectations: {configExpectations};
    Description: {configDescription}.
""")

# COMMAND ----------

# DBTITLE 1,Ingestion logic
(spark.readStream
 .format("cloudFiles")
 .options(**configReadOptions)
#  .schema(bronzeSourceSchema)
 .load(configSourceLocation)
 .withColumn("LoadDate", current_timestamp())
 .withColumn("SourceFileName", input_file_name())
 .writeStream
 .options(**configWriteOptions)
 .trigger(once=True)
 .table(configTargetTable)
)
