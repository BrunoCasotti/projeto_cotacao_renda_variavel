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
CotacoesDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/raw/cotacao_diaria")

# COMMAND ----------

CotacoesDFSerial.display()

# COMMAND ----------

#Seleção dos dados
CotacoesDFSerialSelected = CotacoesDFSerial.select("Ticker", "Data", "Ano", "Cotacao_Abertura", "Cotacao_Fechamento", "Cotacao_Max", "Cotacao_Min", "Volume")

# COMMAND ----------

#Tratamento do nome dos campos
CotacoesDFSerialRenamed = (
    CotacoesDFSerialSelected.withColumnRenamed("Ticker", "ticker")
    .withColumnRenamed("Data", "data_pregao")
    .withColumnRenamed("Cotacao_Abertura", "preco_abertura")
    .withColumnRenamed("Cotacao_Fechamento", "preco_fechamento")
    .withColumnRenamed("Cotacao_Max", "preco_maximo")
    .withColumnRenamed("Cotacao_Min", "preco_minimo")
    .withColumnRenamed("Volume", "volume")
    .withColumnRenamed("Ano", "ano") 
)

# COMMAND ----------

#Alteração do tipo de dados
CotacoesDFSerialChanged = (
    CotacoesDFSerialRenamed.withColumn("data_pregao", to_date(col("data_pregao").cast("string"), 'dd/MM/yyyy'))
    .withColumn("preco_abertura", round(col("preco_abertura").cast("double"),2))
    .withColumn("preco_fechamento", round(col("preco_fechamento").cast("double"),2))
    .withColumn("preco_maximo", round(col("preco_maximo").cast("double"),2))
    .withColumn("preco_minimo", round(col("preco_minimo").cast("double"),2))
    .withColumn("volume", col("volume").cast("int"))
)

# COMMAND ----------

# Selecionando apenas o dia atual
CotacoesDFSerialDay = (
    CotacoesDFSerialChanged.groupBy("ticker","ano","preco_abertura","preco_fechamento","preco_maximo","preco_minimo","volume")
    .agg(max(col("data_pregao")).alias("data_pregao"))
)

# COMMAND ----------

# Ordenando as colunas
CotacoesDFSerialDay = CotacoesDFSerialDay.select("ticker", "data_pregao", "ano", "preco_abertura", "preco_fechamento", "preco_maximo", "preco_minimo", "volume")

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

(CotacoesDFSerialDay
 .write
 .format("parquet")
 .partitionBy("data_pregao")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/silver/cotacao/cotacao_diaria")
)
