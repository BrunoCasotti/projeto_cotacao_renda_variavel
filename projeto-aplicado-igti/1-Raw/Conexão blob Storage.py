# Databricks notebook source
# MAGIC %md # 1. Conexão com o Azure Blob Storage
# MAGIC 
# MAGIC ----

# COMMAND ----------

dbutils.fs.mount(
source = "wasbs://azparquetprjapl@dlsprojetoaplicadoigti.blob.core.windows.net",
mount_point = "/mnt/azparquetprjapl",
extra_configs = {"fs.azure.account.key.dlsprojetoaplicadoigti.blob.core.windows.net":"VImCCHQ0AdFkotOVSW/lVrqol0TlV1Jq4+PjEbNbK6DEFGjpvrG9LxEy6ReADBB2aBXXv2jsXhpC+AStROYvMA=="})
