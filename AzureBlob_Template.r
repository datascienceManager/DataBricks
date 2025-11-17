# Databricks notebook source
# MAGIC %python
# MAGIC
# MAGIC import pandas as pd
# MAGIC import pyspark
# MAGIC from pyspark.sql import functions as F
# MAGIC from pyspark.sql.functions import col, when
# MAGIC from pyspark.sql.functions import sum
# MAGIC from pyspark.sql.functions import countDistinct
# MAGIC from datetime import date, datetime, timedelta
# MAGIC from pyspark.sql.types import IntegerType
# MAGIC
# MAGIC
# MAGIC from azure.core.credentials import AzureNamedKeyCredential
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC import xlsxwriter
# MAGIC import io
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %python
# MAGIC
# MAGIC from azure.core.credentials import AzureNamedKeyCredential
# MAGIC from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
# MAGIC import xlsxwriter
# MAGIC import io

# COMMAND ----------

library(data.table)
library(dplyr)
library(lubridate)
library(sparklyr)
library(SparkR)
library(AzureStor)
library(arrow)

# COMMAND ----------

# MAGIC %python
# MAGIC access_key = dbutils.secrets.get(scope="mlstorage7",key="mlman7")
# MAGIC credential = AzureNamedKeyCredential("mlblobman01", access_key)
# MAGIC
# MAGIC account_name = 'mlblobman01'
# MAGIC access_key = dbutils.secrets.get(scope="mlstorage7",key="mlman7")
# MAGIC container_name = 'contentmonthly'
# MAGIC # blob_name = 'test.xlsx'
# MAGIC account_url = 'https://mlblobman01.blob.core.windows.net/contentmonthly'
# MAGIC # blob_service = BaseBlobService(
# MAGIC #     account_name=account_name,
# MAGIC #     account_key=access_key
# MAGIC # )