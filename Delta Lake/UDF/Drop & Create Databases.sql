-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Drop all the tables
-- MAGIC
-- MAGIC

-- COMMAND ----------


DROP DATABASE IF EXISTS f1_processed CASCADE;

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "dbfs:/mnt/Practice/Delta Lake/Processed";


DROP DATABASE IF EXISTS f1_presentation CASCADE;


CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "dbfs:/mnt/Practice/Delta Lake/Presentation";


