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
import numpy as np
   

# COMMAND ----------

# MAGIC %md # 2. Ingestão de dados
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## 2.1. Dados Cotações
# MAGIC 
# MAGIC ----

# COMMAND ----------

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URLTradingView = "https://scanner.tradingview.com/brazil/scan"
body = {"filter":[{"left":"name","operation":"nempty"}],
                             "options":{ "lang":"pt"},
                             "symbols":{
                                            "query":{ "types":[]},
                                 "tickers":[]
                              }, 
                                 "columns": 
                                        ["name","close","open","beta_1_year","volume","high","low"],
                                 "sort":{ "sortBy":"name","sortOrder":"asc"}
         }

# COMMAND ----------

response = (
    requests
    .post(URLTradingView, json=body)
    .content
)
json_data = json.loads(response)
dados = pd.json_normalize(json_data['data'])

# COMMAND ----------

# Ticker = dados.d[1][0]
# CotacaoFechamento = dados.d[1][1]
# CotacaoAbertura = dados.d[1][2]
# Beta = dados.d[1][3]
# Volume = dados.d[1][4]
# CotacaoMax = dados.d[1][5]
# CotacaoMin = dados.d[1][6]

# COMMAND ----------

rows = []

for stocks in range(json_data['totalCount']):

        rows.append([dados.d[stocks][0],
                    dados.d[stocks][1],
                    dados.d[stocks][2],
                    dados.d[stocks][3],
                    dados.d[stocks][4],
                    dados.d[stocks][5],
                    dados.d[stocks][6]])

# COMMAND ----------

df_final = pd.DataFrame(rows, columns=["Ticker", "Cotacao_Fechamento", "Cotacao_Abertura", "Beta", "Volume", "Cotacao_Max", "Cotacao_Min"])

# COMMAND ----------

# df_final["Ticker"] = df_final["Ticker"].astype(str)
# df_final["Cotacao_Fechamento"] = df_final["Cotacao_Fechamento"].astype(float)
# df_final["Cotacao_Abertura"] = df_final["Cotacao_Abertura"].astype(float)
# df_final["Beta"] = df_final["Beta"].astype(float)
# df_final["Volume"] = df_final["Volume"].fillna(0)
# df_final["Volume"] = df_final["Volume"].astype(int)
# df_final["Cotacao_Max"] = df_final["Cotacao_Max"].astype(float)
# df_final["Cotacao_Min"] = df_final["Cotacao_Min"].astype(float)

# COMMAND ----------

df_final["Data"] = pd.to_datetime("today")
df_final["Ano"] = df_final["Data"].dt.year
df_final["Data"] = pd.to_datetime("today").strftime("%d/%m/%Y")

# COMMAND ----------

CotacoesDiariaDFSerial = spark.createDataFrame(df_final)

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

(CotacoesDiariaDFSerial
 .write
 .format("parquet")
 .partitionBy("Ano")
 .mode("append")
 .save("dbfs:/mnt/azparquetprjapl/raw/cotacao_diaria")
)
