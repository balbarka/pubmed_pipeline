# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC This notebook is to replicate issues with UDFs. It will include multiple examples of module imports within UDF.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, FloatType, IntegerType

# Create a simple dataframe with one column and three entries
dat = spark.createDataFrame([(1,),(2,),(3,)], ['index',])

# COMMAND ----------

# Define the UDF function
@udf(returnType=StringType())
def boto_version():
    import boto3
    return boto3.__version__

display(dat.withColumn('boto3_version', boto_version()))

# COMMAND ----------

# Define the UDF function
@udf(returnType=StringType())
def boto_version():
    import bs4
    return boto3.__version__

display(dat.withColumn('boto3_version', boto_version()))

# COMMAND ----------

# Define the UDF function
@udf(returnType=StringType())
def bs4_registry_class():
    import bs4
    return bs4.builder_registry.__class__

display(dat.withColumn('bs4_registry_class', bs4_registry_class()))

# COMMAND ----------

# Define the UDF function
@udf(returnType=FloatType())
def math_pi():
    import math
    return math.pi

display(dat.withColumn('math_pi', math_pi()))

# COMMAND ----------

# Define the UDF function
@udf(returnType=IntegerType())
def plus_one(x: int):
    return x+1

display(dat.withColumn('plus_one', plus_one('index')))

# COMMAND ----------

# Define the UDF function
@udf(returnType=StringType())
def pandas_series_lookup(i: int):
    import pandas as pd
    return pd.Series(['one','two','three'])[i-1]

display(dat.withColumn('pandas_series_lookup', pandas_series_lookup('index')))
