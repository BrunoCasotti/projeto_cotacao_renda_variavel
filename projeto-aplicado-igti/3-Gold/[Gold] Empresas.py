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
AcoesDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/acoes")
ETFDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/etf")
FIIDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/silver/fii")

# COMMAND ----------

#União das tabelas
EmpresasDFSerial = (AcoesDFSerial.union(ETFDFSerial)
                                 .union(FIIDFSerial)
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

(EmpresasDFSerial
 .write
 .format("parquet")
 .partitionBy("tipo_empresa","ano_aprovado")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/gold/empresas")
)
