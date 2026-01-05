# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest results.json file
# MAGIC

# COMMAND ----------


dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")


# COMMAND ----------


dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC
# MAGIC %run "../Include/configuration"

# COMMAND ----------

# MAGIC %run "../Include/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the Json file using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC Step 2 - Rename columns and add new columns
# MAGIC #### 1. Rename driverId and raceId
# MAGIC #### 1. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_with_ingestion_date_df = pit_stops_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------


from pyspark.sql.functions import lit

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')


# COMMAND ----------

#%sql
#Create Database f1_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT file_date,count(*) FROM f1_processed.pit_stops
# MAGIC group by file_date

# COMMAND ----------


