# Databricks notebook source
# MAGIC %md # 1. Bibliotecas
# MAGIC 
# MAGIC --------------

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # 2. Tratamento de dados
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## 2.1. Dados de Cotações
# MAGIC 
# MAGIC ----

# COMMAND ----------

#Conexão
CotacoesDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/raw/cotacao_historica/parquet")

# COMMAND ----------

#Seleção dos dados
CotacoesDFSerialSelected = CotacoesDFSerial.select("cod_negociacao", "data_pregao", "Ano", "preco_abertura", "preco_ultimo_negocio", "preco_maximo", "preco_minimo", "volume_total_negociado")

# COMMAND ----------

#Tratamento do nome dos campos
CotacoesDFSerialRenamed = (
    CotacoesDFSerialSelected.withColumnRenamed("cod_negociacao", "ticker")
    .withColumnRenamed("preco_ultimo_negocio", "preco_fechamento")
    .withColumnRenamed("volume_total_negociado", "volume")
    .withColumnRenamed("Ano", "ano") 
)

# COMMAND ----------

#Alteração do tipo de dados
CotacoesDFSerialChanged = (
    CotacoesDFSerialRenamed.withColumn("data_pregao", to_date(col("data_pregao").cast("string"), 'yyyyMMdd'))
    .withColumn("preco_abertura", round(col("preco_abertura").cast("double"),2))
    .withColumn("preco_fechamento", round(col("preco_fechamento").cast("double"),2))
    .withColumn("preco_maximo", round(col("preco_maximo").cast("double"),2))
    .withColumn("preco_minimo", round(col("preco_minimo").cast("double"),2))
    .withColumn("volume", col("volume").cast("int"))
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

(CotacoesDFSerialChanged
 .write
 .format("parquet")
 .partitionBy("ano", "data_pregao")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_historica")
)
