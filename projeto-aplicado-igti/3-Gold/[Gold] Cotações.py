# Databricks notebook source
# MAGIC %md # 1. Bibliotecas
# MAGIC 
# MAGIC --------------

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # 2. União das tabelas
# MAGIC 
# MAGIC ----

# COMMAND ----------

# #Conexão - carga full
# HistoricoDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_historica")
# DiarioDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_diaria")

# #União das tabelas
# CotacoesDFSerial = (HistoricoDFSerial.union(DiarioDFSerial.select(HistoricoDFSerial.columns)))

# COMMAND ----------

#Conexão - carga diária/incremental
DiarioDFSerial = (
    spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_diaria")
    .select("ticker", "preco_abertura", "preco_fechamento", "preco_maximo", "preco_minimo", "volume", "ano", "data_pregao")
)

# COMMAND ----------

# MAGIC %md # 3. Gravação dos dados
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC - Blob Storage
# MAGIC - Formato Parquet

# COMMAND ----------

# MAGIC %md ## 3.1. Commit
# MAGIC 
# MAGIC ----

# COMMAND ----------

# # Gravação da carga full
# (CotacoesDFSerial
#  .write
#  .format("parquet")
#  .partitionBy("ano", "data_pregao")
#  .mode("overwrite")
#  .save("dbfs:/mnt/azparquetprjapl/gold/cotacao")
# )

# COMMAND ----------

# Gravação da carga diária/incremental
(DiarioDFSerial
 .write
 .format("parquet")
 .partitionBy("ano", "data_pregao")
 .mode("append")
 .save("dbfs:/mnt/azparquetprjapl/gold/cotacao")
)
