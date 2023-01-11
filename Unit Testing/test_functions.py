# Databricks notebook source
# MAGIC %pip install pytest

# COMMAND ----------

# DBTITLE 1,Imports
import pytest
from pyspark.sql import Row, SparkSession
import pandas as pd
# from functions import *

# COMMAND ----------

# MAGIC %run "/Repos/l_l@adatis.co.uk/Databricks-Code-Repo/Unit Testing/functions"

# COMMAND ----------

# DBTITLE 1,Create Spark session
# Because this file is not a Databricks notebook, you must create a Spark session. 
# Databricks notebooks create a Spark session for you by default.
spark = SparkSession.builder.appName('integrity-tests').getOrCreate()

# COMMAND ----------

# DBTITLE 1,Define tests
def test_lowerCaseColNames():
    # ASSEMBLE
    test_data = [
        {
            "Id": 1,
            "FirstName": "John",
            "LastName": "Smith",
            "Occupation": "Software Engineer",
            "Country": "United Kingdom",
            "City": "Mattingley",
            "Street": "16 Worthy Lane",
            "ZipCode": "RG27 4WW",
            "PhoneNumber": "077 2183 2343"
        }, 
        {
            "Id": 2,
            "FirstName": "Christopher",
            "LastName": "Johnson",
            "Occupation": "Software Engineer",
            "Country": "United States",
            "City": "Winterhaven",
            "Street": "4329 Alpha Avenue",
            "ZipCode": "92283",
            "PhoneNumber": "904-213-7323"
        }
    ]

#     spark = SparkSession.builder().getOrCreate()
    
    test_df = spark.createDataFrame(data=test_data)
    
    # ACT
    output_df = lowerCaseColNames(df=test_df)
    # Convert DataFrame to Pandas to use Padnas helper test functions
    output_df_pd = output_df.toPadnas()
    
    expected_output_df = pd.DataFrame({
        "id":[1, 2],
        "firstname":["John", "Christopher"],
        "lastname":["Smith", "Johnson"],
        "occupation":["Software Engineer", "Software Engineer"],
        "country":["United Kingdom", "United States"],
        "city":["Mattingley", "Winterhaven"],
        "street":["16 Worthy Lane", "4329 Alpha Avenue"],
        "zipcode":["RG27 4WW", "92283"],
        "phonenumber":["077 2183 2343", "904-213-7323"]
    })
    
    # ASSERT
    pd.testing.assert_frame_equal(expected_output_df, output_df_pd, check_exact=True)

# COMMAND ----------


