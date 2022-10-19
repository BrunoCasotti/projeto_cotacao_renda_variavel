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

#Conexão
AcoesDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/raw/acoes")

# COMMAND ----------

#Seleção dos dados
AcoesDFSerialSelected = (
    AcoesDFSerial.select(lit("ACAO").alias("tipo_empresa"),
                         col("hasCommom").alias("escriturador_empresa"),
                         col("tradingName").alias("nome_empresa"),
                         col("code").alias("ticker_empresa"),
                         col("quotedPerSharSince").alias("data_aprovado"),
                         col("quotedPerSharSinceYear").alias("ano_aprovado")
                        )
)

# COMMAND ----------

#Alteração do tipo de dados e remoção de espaços
AcoesDFSerialChanged = (
    AcoesDFSerialSelected.withColumn("ano_aprovado", col("ano_aprovado").cast("int"))
    .withColumn("data_aprovado", to_date(col("data_aprovado"), 'dd/MM/yyyy'))
    .withColumn("ticker_empresa", trim(col("ticker_empresa")))
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

(AcoesDFSerialChanged
 .write
 .format("parquet")
 .partitionBy("ano_aprovado")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/silver/acoes")
)
