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
FIIDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/raw/fii")

# COMMAND ----------

FIIDFSerialRenamed = (
    FIIDFSerial.withColumnRenamed("shareHolder.shareHolderName", "shareHolderName")
    .withColumnRenamed("detailFund.tradingCode", "tradingCode")
    .withColumnRenamed("detailFund.companyName", "companyName")
    .withColumnRenamed("detailFund.quotaDateApproved", "quotaDateApproved")
)

# COMMAND ----------

#Seleção dos dados
FIIDFSerialSelected = (
    FIIDFSerialRenamed.select(lit("FII").alias("tipo_empresa"),
                         col("shareHolderName").alias("escriturador_empresa"),
                         col("companyName").alias("nome_empresa"),
                         col("tradingCode").alias("ticker_empresa"),
                         col("quotaDateApproved").alias("data_aprovado"),
                         col("quotaYearApproved").alias("ano_aprovado")
                        )
)

# COMMAND ----------

#Alteração do tipo de dados e remoção de espaços
FIIDFSerialChanged = (
    FIIDFSerialSelected.withColumn("ano_aprovado", col("ano_aprovado").cast("int"))
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

(FIIDFSerialChanged
 .write
 .format("parquet")
 .partitionBy("ano_aprovado")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/silver/fii")
)
