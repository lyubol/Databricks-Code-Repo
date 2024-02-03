-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### 1. Insert metadata below:

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW TempView_LatestEntityMetadata (
  EntityId,
  EntityName,
  EntityFormat,
  EntityReadOptions,
  EntityExpectations
) AS
VALUES
  -- EntityId, EntityName, EntityFormat, EntityReadOptions, EntityExpectations
  (1, 'Account', 'csv', '{"Header": True, "inferSchema": True}', '{"ValidId": "Id IS NOT NULL"}'),
  (2, 'Product', 'csv', '{"Header": True, "inferSchema": True}', '{"ValidId": "Id IS NOT NULL"}'),
  (3, 'Customer', 'csv', '{"Header": True, "inferSchema": True}', '{"ValidId": "Id IS NOT NULL"}')


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
  EntityFormat VARCHAR(10),
  EntityReadOptions VARCHAR(255),
  EntityExpectations VARCHAR(1000)
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
  Target.EntityFormat = Source.EntityFormat,
  Target.EntityReadOptions = Source.EntityReadOptions,
  Target.EntityExpectations = Source.EntityExpectations
  WHEN NOT MATCHED BY TARGET THEN
INSERT
  (
    EntityId,
    EntityName,
    EntityFormat,
    EntityReadOptions,
    EntityExpectations
  )
VALUES
  (
    Source.EntityId,
    Source.EntityName,
    Source.EntityFormat,
    Source.EntityReadOptions,
    Source.EntityExpectations
  )
  WHEN NOT MATCHED BY SOURCE THEN DELETE

-- COMMAND ----------

-- SELECT * FROM Metadata.entitymetadata
