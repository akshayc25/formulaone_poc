# Databricks notebook source
# MAGIC %md
# MAGIC #### Access azure data lake using access key
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file
# MAGIC
# MAGIC

# COMMAND ----------


spark.conf.set("fs.azure.account.key.marstorageaccount.dfs.core.windows.net","")
dbutils.ls.fs("adfss://")

# COMMAND ----------


