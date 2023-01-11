# Databricks notebook source
# DBTITLE 1,Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# DBTITLE 1,Create Spark session
# Because this file is not a Databricks notebook, you must create a Spark session. 
# Databricks notebooks create a Spark session for you by default.
spark = SparkSession.builder.appName('integrity-tests').getOrCreate()

# COMMAND ----------

# DBTITLE 1,Define functions
# Converts all column names to lower case
def lowerCaseColNames(input_df):
    output_df = input_df.toDF(*[c.lower() for c in input_df.columns])
    return output_df

# Converts all column names to upper case
def upperCaseColNames(input_df):
    output_df = input_df.toDF(*[c.upper() for c in input_df.columns])
    return output_df

# Removes spaces in column names
def removeSpacesColNames(input_df):
    output_df = input_df.toDF(*[c.replace(" ", "") for c in input_df.columns])
    return output_df

# COMMAND ----------

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
