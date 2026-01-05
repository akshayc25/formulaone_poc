# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC create table emp  using csv options (path "dbfs:/FileStore/employees.csv",header "true")
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp

# COMMAND ----------

# MAGIC %sql
# MAGIC Select city , avg(salary) from emp group by city

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from emp

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------


