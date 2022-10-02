# Databricks notebook source
# MAGIC %md # 1. Bibliotecas
# MAGIC 
# MAGIC --------------

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md # 2. Ingestão de dados
# MAGIC 
# MAGIC ----

# COMMAND ----------

# MAGIC %md ## 2.1. Dados Cotações
# MAGIC 
# MAGIC ----

# COMMAND ----------

CotacaoDFSerial = spark.read.option("encoding", "ISO-8859-1").text("dbfs:/mnt/azparquetprjapl/raw/cotacao_historica/csv")

# COMMAND ----------

CotacaoDFSerialChanged = (
    CotacaoDFSerial.select(
    CotacaoDFSerial.value.substr(1,2).alias("tipo_registro"),
    CotacaoDFSerial.value.substr(3,8).alias("data_pregao"),
    CotacaoDFSerial.value.substr(11,1).alias("cod_bdi"),
    CotacaoDFSerial.value.substr(13,12).alias("cod_negociacao"),
    CotacaoDFSerial.value.substr(25,3).alias("tipo_mercado"),
    CotacaoDFSerial.value.substr(28,12).alias("noma_empresa"),
    CotacaoDFSerial.value.substr(40,10).alias("especificacao_papel"),
    CotacaoDFSerial.value.substr(50,3).alias("prazo_dias_merc_termo"),
    CotacaoDFSerial.value.substr(53,4).alias("moeda_referencia"),
    CotacaoDFSerial.value.substr(57,11).alias("preco_abertura"),
    CotacaoDFSerial.value.substr(70,11).alias("preco_maximo"),
    CotacaoDFSerial.value.substr(83,11).alias("preco_minimo"),
    CotacaoDFSerial.value.substr(96,11).alias("preco_medio"),
    CotacaoDFSerial.value.substr(109,11).alias("preco_ultimo_negocio"),
    CotacaoDFSerial.value.substr(122,11).alias("preco_melhor_oferta_compra"),
    CotacaoDFSerial.value.substr(135,11).alias("preco_melhor_oferta_venda"),
    CotacaoDFSerial.value.substr(148,5).alias("numero_negocios"),
    CotacaoDFSerial.value.substr(153,18).alias("quantidade_papeis_negociados"),
    CotacaoDFSerial.value.substr(171,16).alias("volume_total_negociado"),
    CotacaoDFSerial.value.substr(189,11).alias("preco_exercicio"),
    CotacaoDFSerial.value.substr(202,1).alias("ìndicador_correcao_precos"),
    CotacaoDFSerial.value.substr(203,8).alias("data_vencimento"),
    CotacaoDFSerial.value.substr(211,7).alias("fator_cotacao"),
    CotacaoDFSerial.value.substr(218,7).alias("preco_exercicio_pontos"),
    CotacaoDFSerial.value.substr(231,12).alias("codigo_isin"),
    CotacaoDFSerial.value.substr(243,3).alias("num_distribuicao_papel"),
    ).filter(col("tipo_registro") == "01")
)

# COMMAND ----------

CotacaoDFSerialTransformed = CotacaoDFSerialChanged.withColumn("Ano", substring("data_pregao", 1,4))

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

(CotacaoDFSerialTransformed
 .write
 .format("parquet")
 .partitionBy("Ano", "data_pregao")
 .mode("overwrite")
 .save("dbfs:/mnt/azparquetprjapl/raw/cotacao_historica/parquet")
)
