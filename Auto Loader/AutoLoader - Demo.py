# Databricks notebook source
# DBTITLE 1,Define schema
from pyspark.sql.types import *
import json

# In a real world example, instead of defining the schema like that, we will be connecting to a database to obtain the schema. 
json_schema = """
{
    "fields": [
      {
            "metadata": {},
            "name": "OrderID",
            "nullable": true,
            "type": "integer"
      },
      {
            "metadata": {},
            "name": "Product",
            "nullable": true,
            "type": "string"
      },
      {
            "metadata": {},
            "name": "QuantityOrdered",
            "nullable": true,
            "type": "integer"
      },
      {
            "metadata": {},
            "name": "PriceEach",
            "nullable": true,
            "type": "decimal"
      },
      {
            "metadata": {},
            "name": "OrderDate",
            "nullable": true,
            "type": "timestamp"
      },
      {
            "metadata": {},
            "name": "PurchaseAddress",
            "nullable": true,
            "type": "string"
      }
    ],
    "type": "struct"
}"""

schema = StructType.fromJson(json.loads(json_schema))

# COMMAND ----------

# DBTITLE 1,Setup connection to storage
# The following configuration is using an access key
spark.conf.set("fs.azure.account.key.<storage_account>.blob.core.windows.net", "<access_key>")

# COMMAND ----------

# DBTITLE 1,Initialize tables
df = spark.read.format("csv").schema(schema).load("wasbs://<container>@<storage_account>.blob.core.windows.net/")

df.write.format("delta").save("/mnt/<storage_account>/<container>/<path>/")

# COMMAND ----------

# DBTITLE 1,Define SAS connection string
# Queue SAS Key

queuesas = "<sas_connection_string>"

# COMMAND ----------

# DBTITLE 1,cloudFiles configuration
cloudfile = {
    "cloudFiles.subscriptionId": "<subscriptionId>",
    "cloudFiles.connectionString": queuesas,
    "cloudFiles.format": "<file_format>",
    "cloudFiles.tenantId": "<tenantId>",
    "cloudFiles.clientId": "<clientId>",
    "cloudFiles.clientSecret": "<clientSecret>",
    "cloudFiles.resourceGroup": "<resourceGroup>"
}

# COMMAND ----------

(df = 
 spark.readStream
 .format("cloudFiles")
 .options(**cloudfile)
 .option("cloudFiles.validateOptions", "false")
 .schema(schema)
 .load("wasbs://<container>@<storage_account>.blob.core.windows.net/")
)

# COMMAND ----------

(df.writeStream
 .format("delta")
 .option("checkpointLocation","<path_to_checkpoint_location>")
 .start("<path_to_target_destination>")
)

# COMMAND ----------

# DBTITLE 1,Trigger once
# The triger once option allows the stream to check for any unprocessed files, since the last time it ran and process them. It will initialize a stream, process all unprocessed files and will close the stream.
(df.writeStream
 .format("delta")
 .trigger(once=True)
 .option("checkpointLocation","<path_to_checkpoint_location>")
 .start("<path_to_target_destination>")
)
