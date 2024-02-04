-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 1. Insert metadata below:

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TempView_LatestEntityMetadata (
  EntityId,
  EntityName,
  ReadOptions,
  WriteOptions,
  SourceLocation,
  TargetTable,
  Expectations,
  Description
) AS
VALUES
  -- EntityId, EntityName, ReadOptions, WriteOptions, SourceLocation, TargetTable, Expectations, Description
  (1, 'Sales', '{"cloudFiles.format":"csv", "cloudFiles.schemaEvolutionMode":"rescue", "cloudFiles.schemaLocation": "/mnt/adlslirkov/raw/DLTFreamwork/Bronze/Sales/Schema/", "Header":"True", "inferSchema":"True"}', '{"checkpointLocation":"/mnt/adlslirkov/raw/DLTFreamwork/Bronze/Sales/Checkpoint/", "mergeSchema":"True"}', '/mnt/adlslirkov/raw/AutoLoaderInput/', 'Bronze.Sales', '{"ValidOrderId":"OrderId IS NOT NULL", "ValidProduct":"Product IS NOT NULL", "ValidQuantityOrdered":"QuantityOrdered IS NOT NULL AND QuantityOrdered > 0", "ValidPrice":"PriceEach IS NOT NULL AND PriceEach > 0", "ValidOrderDate":"OrderDate IS NOT NULL", "ValidPurchaseAddress":"PurchaseAddress IS NOT NULL"}', 'The Sales entity')
  -- (2, 'Product', '{"Header": "True", "inferSchema": "True"}', '{"ValidId": "Id IS NOT NULL"}', 'The Product entity'),
  -- (3, 'Customer', '{"Header": "True", "inferSchema": "True"}', '{"ValidId": "Id IS NOT NULL"}', 'The Customer entity')


-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Check if column EntityId contains duplicates
-- MAGIC duplicatesEntityId = spark.sql("""
-- MAGIC     SELECT EntityId, COUNT(EntityId) AS EntityIdCount 
-- MAGIC     FROM TempView_LatestEntityMetadata 
-- MAGIC     GROUP BY EntityId 
-- MAGIC     HAVING EntityIdCount > 1
-- MAGIC """).count()
-- MAGIC
-- MAGIC # Check if column EntityName contains duplicates
-- MAGIC duplicatesEntityName = spark.sql("""
-- MAGIC     SELECT EntityName, COUNT(EntityName) AS EntityNameCount 
-- MAGIC     FROM TempView_LatestEntityMetadata 
-- MAGIC     GROUP BY EntityName 
-- MAGIC     HAVING EntityNameCount > 1
-- MAGIC """).count()
-- MAGIC
-- MAGIC if duplicatesEntityId > 0:
-- MAGIC     raise Exception('There are duplicates in column EntityId!')
-- MAGIC elif duplicatesEntityName > 0:
-- MAGIC     raise Exception('There are duplicates in column EntityName!')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. Apply latest updates

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS Metadata
  COMMENT 'Entities Metadata';

USE Metadata;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS Metadata.EntityMetadata (
  EntityId INT,
  EntityName VARCHAR(100),
  ReadOptions VARCHAR(1000),
  WriteOptions VARCHAR(1000),
  SourceLocation VARCHAR(1000),
  TargetTable VARCHAR(100),
  Expectations VARCHAR(1000),
  Description VARCHAR(1000)
);

-- COMMAND ----------

MERGE INTO Metadata.EntityMetadata AS Target 
USING TempView_LatestEntityMetadata AS Source 
ON (
  Target.EntityName = Source.EntityName
  AND Target.EntityId = Source.EntityId
)
WHEN MATCHED THEN
UPDATE
SET
  Target.ReadOptions = Source.ReadOptions,
  Target.WriteOptions = Source.WriteOptions,
  Target.SourceLocation = Source.SourceLocation,
  Target.TargetTable = Source.TargetTable,
  Target.Expectations = Source.Expectations,
  Target.Description = Source.Description
  WHEN NOT MATCHED BY TARGET THEN
INSERT
  (
    EntityId,
    EntityName,
    ReadOptions,
    WriteOptions,
    SourceLocation,
    TargetTable,
    Expectations,
    Description
  )
VALUES
  (
    Source.EntityId,
    Source.EntityName,
    Source.ReadOptions,
    Source.WriteOptions,
    Source.SourceLocation,
    Source.TargetTable,
    Source.Expectations,
    Source.Description
  )
  WHEN NOT MATCHED BY SOURCE THEN DELETE

-- COMMAND ----------

-- SELECT * FROM Metadata.EntityMetadata

-- COMMAND ----------

SELECT COUNT(*) FROM Bronze.Sales

-- COMMAND ----------


