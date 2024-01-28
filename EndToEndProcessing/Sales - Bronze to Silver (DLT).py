# Databricks notebook source
import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

bronzeDeltaTablePath = spark.conf.get("bronzeDeltaTablePath")
# silverDeltaTablePath = spark.conf.get("silverDeltaTablePath")

silverDeltaTablePath = "/mnt/adlslirkov/raw/SalesEndToEnd/Silver/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC from Bronze to Silver

# COMMAND ----------

# bronzeViewConfig = {
#     "cloudFiles.format": "delta",
#     "cloudFiles.inferSchema": "true",
# }

silverSourceSchema = StructType([
    StructField("OrderId", IntegerType()),
    StructField("ProductName", StringType()),
    StructField("QuantityOrdered", IntegerType()),
    StructField("PriceEach", DoubleType()),
    StructField("OrderDateTime", TimestampType()),
    StructField("PurchaseAddress", StringType()),
    StructField("LoadDate", TimestampType())
])

@dlt.view(
    name = "SalesBronze_View"
)
def salesbronze_view():
    return(
        spark.readStream
        .format("delta")
        # .options(**bronzeViewConfig)
        .load(bronzeDeltaTablePath)
        .where(col("_rescued_data").isNull())
        .withColumn("OrderDate", regexp_replace(col("OrderDate"), "2019", "19"))
        .withColumn("OrderDate", to_timestamp("OrderDate", "MM/dd/yy HH:mm"))
        .withColumn("LoadDate", current_timestamp())
        .select(
            col("OrderId"),
            col("Product").alias("ProductName"),
            col("QuantityOrdered"),
            col("PriceEach"),
            col("OrderDate").alias("OrderDateTime"),
            col("PurchaseAddress"),
            col("LoadDate")
        )
    )

dlt.create_target_table(
    name = "SalesSilver",
    comment = "This is the Silver layer delta table for the sales data",
    path = "/mnt/adlslirkov/raw/SalesEndToEnd/Silver/",
    schema = silverSourceSchema,
    expect_all_or_fail = {"PopulatedId": "OrderId IS NOT NULL", "PopulatedProductName": "ProductName IS NOT NULL", "PopulatedQuantityOrdered": "QuantityOrdered IS NOT NULL", "ValidQuantityOrdered": "QuantityOrdered > 0", "PopulatedPriceEach": "PriceEach IS NOT NULL", "ValidPriceEach": "PriceEach > 0", "PopulatedOrderDateTime": "OrderDateTime IS NOT NULL", "PopulatedPurchaseAddress": "PurchaseAddress IS NOT NULL"}
)

dlt.apply_changes(
    target = "SalesSilver",
    source = "SalesBronze_View",
    keys = ["OrderId"],
    sequence_by = col("OrderDateTime"),
    stored_as_scd_type = 1
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### CDC from Silver to Gold

# COMMAND ----------

# TBC...

# COMMAND ----------

# spark.sql(
#     """
#     SELECT * FROM Sales.SalesSilver
#     """
# ).display()
