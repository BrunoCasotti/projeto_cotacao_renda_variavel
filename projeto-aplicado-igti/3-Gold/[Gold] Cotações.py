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

#Conexão
HistoricoDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_historica")
DiarioDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_diaria")

# COMMAND ----------

#União das tabelas
CotacoesDFSerial = (HistoricoDFSerial.union(DiarioDFSerial))

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

(CotacoesDFSerial
 .write
 .format("parquet")
 .partitionBy("ano")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/gold/cotacao")
)
