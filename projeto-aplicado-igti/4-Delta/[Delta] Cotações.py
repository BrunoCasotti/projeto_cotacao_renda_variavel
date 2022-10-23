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

# Conexão com a tabela de cotações
CotacoesDFSerial = spark.read.parquet("dbfs:/mnt/azparquetprjapl/gold/cotacao")

# COMMAND ----------

# Conexão com a tabela de empresa
EmpresaDFSerial = spark.read.table("projeto_aplicado.empresas").select(col("ticker_empresa").alias("ticker"), "tipo_empresa")

# COMMAND ----------

# Trazendo a informação de tipo_empresa
CotacoesJoined = CotacoesDFSerial.join(EmpresaDFSerial, ["ticker"], "left")

# COMMAND ----------

# Criando a coluna id_ticker para relações não nulas
CotacoesNotNull = (
 CotacoesJoined
 .filter(col("tipo_empresa").isNotNull())
 .withColumn("id_ticker", col("ticker"))
 .drop(col("tipo_empresa"))
)

# COMMAND ----------

# Criando a coluna id_ticker para relações nulas
CotacoesNull = (
 CotacoesJoined
 .filter(col("tipo_empresa").isNull())
 .withColumn("id_ticker", col("ticker").substr(1,4))
 .drop(col("tipo_empresa"))
)

# COMMAND ----------

# Unindo novamente o dataframe
CotacoesDFNew = CotacoesNotNull.union(CotacoesNull)

# COMMAND ----------

# MAGIC %md # 3. Gravação dos dados
# MAGIC 
# MAGIC ----
# MAGIC 
# MAGIC - Databricks
# MAGIC - Formato Delta

# COMMAND ----------

(CotacoesDFNew
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("projeto_aplicado.cotacao")
)
