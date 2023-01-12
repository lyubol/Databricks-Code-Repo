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
  ('Sales_January_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- February
  ('Sales_February_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- March
  ('Sales_March_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- April
  ('Sales_April_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- May
  ('Sales_May_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- June
  ('Sales_June_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- July
  ('Sales_July_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- August
  ('Sales_August_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- September
  ('Sales_September_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- October
  ('Sales_October_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- November
  ('Sales_November_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}'),
  -- December
  ('Sales_December_2019', '{"Header":"True","Sep":",","inferSchema":"True"}', '{"OrderID is not missing":"OrderID IS NOT NULL","Product name is not missing":"Product IS NOT NULL","QuantityOrdered >= 0":"QuantityOrdered >= 0","PriceEach is not missing":"PriceEach IS NOT NULL","PriceEach > 0":"PriceEach > 0","PurchaseAddress is not missing":"PurchaseAddress IS NOT NULL"}')

-- COMMAND ----------

SELECT * FROM DLTConfig.sources
