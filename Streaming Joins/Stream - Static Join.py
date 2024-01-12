# Databricks notebook source
# MAGIC %md
# MAGIC Create Delta Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create empty Orders fact table
# MAGIC USE Test;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW temp_sales
# MAGIC (OrderId INT, Product STRING, QuantityOrdered INT, PriceEach INT, OrderDate STRING, PurchaseAddress STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = '<path>',
# MAGIC   header = 'true',
# MAGIC   delimiter = ','
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS Test.Orders
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   SELECT * FROM temp_sales

# COMMAND ----------

# MAGIC %sql
# MAGIC USE Test;
# MAGIC
# MAGIC -- Create Product dimension
# MAGIC CREATE TABLE IF NOT EXISTS Test.Product
# MAGIC USING DELTA
# MAGIC AS
# MAGIC   SELECT  ROW_NUMBER() OVER(ORDER BY Product) AS ProductId,
# MAGIC           Product                             AS ProductName, 
# MAGIC           MAX(PriceEach)                      AS ProductPrice
# MAGIC   FROM    temp_sales
# MAGIC   GROUP BY Product
# MAGIC   HAVING ProductPrice IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC Stream - Static Join

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO Test.Orders
# MAGIC VALUES 
# MAGIC   -- (150502, "iPhone", 1,	700, "02/18/19 01:35", "866 Spruce St, Portland, ME 04101")
# MAGIC   (159999, "iPhone", 10,	700, "02/18/19 01:35", "866 Spruce St, Portland, ME 04101")
# MAGIC   -- (150503, "AA Batteries (4-pack)",	1, 3.84, "02/13/19 07:24", "18 13th St, San Franisco, CA 94016")
# MAGIC   -- (150504, "27in 4K Gaming Monitor", 1,	389.99,	"02/18/19 09:46", "52 6th St, New York City, NY 10001")

# COMMAND ----------

spark.readStream.table("Test.Orders").createOrReplaceTempView("TEMP_test_orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM TEMP_test_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW TEMP_orders_products
# MAGIC AS
# MAGIC   SELECT * FROM TEMP_test_orders AS o
# MAGIC   LEFT JOIN Test.product AS p
# MAGIC     ON o.Product = p.ProductName

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM TEMP_orders_products
