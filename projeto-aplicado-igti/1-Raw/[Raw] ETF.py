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
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # 2. Ingestão de dados
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## 2.1. Dados ETF's 
# MAGIC 
# MAGIC ----

# COMMAND ----------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

json_request = '{"typeFund":20,"pageNumber":1,"pageSize":1000}'
json_request = json_request.encode('utf-8')
base64Request = b64.b64encode(bytearray(json_request)).decode('utf-8')
    
response = requests.get("https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetListedFundsSIG/" + base64Request, verify=False).content
json_data = json.loads(response)

# COMMAND ----------

data = []
for etf in json_data['results']:
    
    try:
        
        print("Importando " + etf["acronym"])
        
        json_request = '{"typeFund":20,"cnpj":"0","identifierFund":"' + etf["acronym"] +'"}'
        json_request = json_request.encode('utf-8')
        base64Request = b64.b64encode(bytearray(json_request)).decode('utf-8')

        url = 'https://sistemaswebb3-listados.b3.com.br/fundsProxy/fundsCall/GetDetailFundSIG/' + base64Request
        responseETF = requests.get(url, verify=False).content
        json_response = json.loads(responseETF)
        data.append(json_response)
    except:
        continue

# COMMAND ----------

df_detail_pd = pd.json_normalize(data)
df_detail_pd["quotaYearApproved"] = pd.to_datetime(df_detail_pd["detailFund.quotaDateApproved"])
df_detail_pd["quotaYearApproved"] = df_detail_pd["quotaYearApproved"].dt.year

# COMMAND ----------

EtfDFSerial = spark.createDataFrame(df_detail_pd).drop("detailFund.codes", "detailFund.codesOther", "detailFund.segment")

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

(EtfDFSerial
 .write
 .format("parquet")
 .partitionBy("quotaYearApproved")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/raw/etf")
)
