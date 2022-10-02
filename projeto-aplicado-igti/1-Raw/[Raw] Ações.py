# Databricks notebook source
# MAGIC %md # 1. Bibliotecas
# MAGIC 
# MAGIC --------------

# COMMAND ----------

from selenium import webdriver
from bs4 import BeautifulSoup
import time
import json
import requests
import base64 as b64
import urllib3
import pandas as pd
import numpy as np
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # 2. Ingestão de dados
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## 2.1. Dados Ações
# MAGIC 
# MAGIC ----

# COMMAND ----------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

json_request = '{"language":"pt-br","pageNumber":1,"pageSize":3000}'
json_request = json_request.encode('utf-8')
base64Request = b64.b64encode(bytearray(json_request)).decode('utf-8')
    
response = (
    requests
    .get("https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetInitialCompanies/" + base64Request, verify=False)
    .content
)

json_data = json.loads(response)

# COMMAND ----------

json_data

# COMMAND ----------

# data = {}

# json_request = '{"issuingCompany":"RRRP","language":"pt-br"}'
# json_request = json_request.encode('utf-8')
# base64Request = b64.b64encode(bytearray(json_request)).decode('utf-8')

# url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/' + base64Request
# responseStocks = requests.get(url, verify=False).content
# json_response = json.loads(responseStocks)
# data = list(data) + json_response

# COMMAND ----------

# df_detail_pd.display()

# COMMAND ----------

data = {}

for stocks in json_data['results']:
    
    try:
        print("Importando " + stocks["issuingCompany"])

        json_request = '{"issuingCompany":"'+ stocks["issuingCompany"] +'","language":"pt-br"}'
        json_request = json_request.encode('utf-8')
        base64Request = b64.b64encode(bytearray(json_request)).decode('utf-8')

        url = 'https://sistemaswebb3-listados.b3.com.br/listedCompaniesProxy/CompanyCall/GetListedSupplementCompany/' + base64Request
        responseStocks = requests.get(url, verify=False).content
        json_response = json.loads(responseStocks)
        data = list(data) + json_response
    except:
        continue

# COMMAND ----------

df_detail_pd = pd.json_normalize(data)
df_detail_pd["quotedPerSharSinceYear"] = pd.to_datetime(df_detail_pd["quotedPerSharSince"], format="%d/%m/%Y", errors = 'coerce')
df_detail_pd["quotedPerSharSinceYear"] = df_detail_pd["quotedPerSharSinceYear"].dt.year

# COMMAND ----------

AcoesDFSerial = spark.createDataFrame(df_detail_pd)

# COMMAND ----------

# MAGIC %md # 3. Gravação dos dados
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC - Blob Storage
# MAGIC - Formato Parquet

# COMMAND ----------

# MAGIC %md ## 3.1. Conexão com o Azure Blob Storage
# MAGIC 
# MAGIC ----

# COMMAND ----------

# dbutils.fs.mount(
# source = "wasbs://azparquetprjapl@dlsprojetoaplicadoigti.blob.core.windows.net",
# mount_point = "/mnt/azparquetprjapl",
# extra_configs = {"fs.azure.account.key.dlsprojetoaplicadoigti.blob.core.windows.net":"VImCCHQ0AdFkotOVSW/lVrqol0TlV1Jq4+PjEbNbK6DEFGjpvrG9LxEy6ReADBB2aBXXv2jsXhpC+AStROYvMA=="})

# COMMAND ----------

# MAGIC %md ## 3.2. Commit
# MAGIC 
# MAGIC ----

# COMMAND ----------

(AcoesDFSerial
 .write
 .format("parquet")
 .partitionBy("quotedPerSharSinceYear")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/raw/acoes")
)
