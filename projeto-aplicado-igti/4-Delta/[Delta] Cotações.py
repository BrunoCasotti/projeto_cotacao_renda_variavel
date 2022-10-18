# Databricks notebook source
# MAGIC %md # 1. Bibliotecas
# MAGIC 
# MAGIC --------------

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # 2. Conexão com a tabela no data storage
# MAGIC 
# MAGIC ----

# COMMAND ----------

#Conexão
CotacoesDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/gold/cotacao")

# COMMAND ----------

# MAGIC %md # 3. Gravação dos dados
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC - Databricks
# MAGIC - Formato Delta

# COMMAND ----------

(CotacoesDFSerial
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_aplicado.cotacao")
)
