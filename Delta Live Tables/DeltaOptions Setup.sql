-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS DLTConfig;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DLTConfig.sources
(
  TableName STRING,
  ReadOptions STRING,
  Expectations STRING
)

-- COMMAND ----------

TRUNCATE TABLE DLTConfig.sources

-- COMMAND ----------

INSERT INTO DLTConfig.sources (TableName, ReadOptions, Expectations)
VALUES 
  -- January
  ('Sales_January_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- February
  ('Sales_February_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),  
  -- March
  ('Sales_March_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- April
  ('Sales_April_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- May
  ('Sales_May_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- June
  ('Sales_June_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- July
  ('Sales_July_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- August
  ('Sales_August_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- September
  ('Sales_September_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- October
  ('Sales_October_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- November
  ('Sales_November_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}'),
  -- December
  ('Sales_December_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"Order ID is not missing":"Order ID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","Quantity ordered >= 0":"Quantity Ordered >= 0","Price Each is not missing":"Price Each IS NOT NULL","Price Each > 0":"Price Each > 0","Purchase Address is not missing":"Purchase Address IS NOT NULL"}')

-- COMMAND ----------

SELECT * FROM DLTConfig.sources
