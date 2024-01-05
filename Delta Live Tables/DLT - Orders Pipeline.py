# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

bronzePath = spark.conf.get("bronzePath")

# COMMAND ----------

# Bronze table
@dlt.table
def orders_bronze():
    return(
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", True)
        .option("header", True)
        .load(bronzePath)
        .withColumn("OrderDate", regexp_replace(col("OrderDate"), "2019", "19"))
        .withColumn("OrderDate", to_timestamp("OrderDate", "MM/dd/yy HH:mm"))
        .select(
            "*",
            current_timestamp().alias("IngestionTimestamp"),
            input_file_name().alias("SourceFileName")
        )
    )

# COMMAND ----------

# Silver table
@dlt.table(
    comment = "Append valid orders only",
    table_properties = {"quality": "silver"}
)
@dlt.expect_or_drop("valid_order_quantity", col("QuantityOrdered") >= 1)
@dlt.expect_or_drop("valid_price", col("PriceEach") >= 1)
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
        .select(
            "OrderId",
            "Product",
            "QuantityOrdered",
            "PriceEach",
            (col("QuantityOrdered") * col("PriceEach")).alias("PriceTotal"),
            "OrderDate",
            year(col("OrderDate")).alias("OrderYear"),
            month(col("OrderDate")).alias("OrderMonth"),
            dayofmonth(col("OrderDate")).alias("OrderDay"),
            "PurchaseAddress",
            "IngestionTimestamp",
            "SourceFileName",
            "_rescued_data"
        )
    )

# COMMAND ----------

# Gold table
@dlt.table(
    comment = "Aggregate by order date",
    table_properties = {"quality": "gold"}
)
def orders_quantity_by_date_gold():
    return(
        dlt.read_stream("orders_silver")
        .groupBy("OrderYear", "OrderMonth", "OrderDay")
        .agg(sum("QuantityOrdered").alias("TotalQuantityOrdered"))
        .select(
            "OrderYear",
            "OrderMonth",
            "OrderDay",
            "TotalQuantityOrdered"
        )
    )
