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


from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")


# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC  

# COMMAND ----------

from pyspark.sql.functions import lit

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Drop the unwanted columns
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

# COMMAND ----------

# MAGIC  %md
# MAGIC ##### De-dupe the dataframe

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])


# COMMAND ----------

display(results_final_df)

# COMMAND ----------


#%sql
   # CREATE DATABASE IF NOT EXISTS f1_processed
#LOCATION "dbfs:/mnt/Practice/Delta Lake/Processed";


# COMMAND ----------

# MAGIC %md 
# MAGIC ###### Step 4 Write output to parquet file

# COMMAND ----------


  spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning","true")

  from delta.tables import DeltaTable
  if (spark._jsparkSession.catalog().tableExists("f1_processed.results")):
    deltaTable = DeltaTable.forPath(spark, "/mnt/Practice/Delta Lake/Processed/results")
    deltaTable.alias("tgt").merge(results_final_df.alias("src"),"tgt.result_id = src.result_id AND tgt.race_id = src.race_id") \
              .whenMatchedUpdateAll()\
              .whenNotMatchedInsertAll()\
              .execute()
  else:
    results_final_df.write.mode("overwrite").partitionBy("race_id").format("delta").saveAsTable("f1_processed.results")




# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1) 
# MAGIC FROM f1_processed.results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT  file_date,Count(*)  FROM f1_processed.results group by file_date

# COMMAND ----------

# MAGIC %md
# MAGIC List the files

# COMMAND ----------

#%fs
#ls dbfs:/user/hive/warehouse/f1_processed.db/results

# COMMAND ----------

# MAGIC %md
# MAGIC Delete the files

# COMMAND ----------

#%fs
#rm -r dbfs:/user/hive/warehouse/f1_processed.db/results

# COMMAND ----------

#%sql
#Drop database f1_processed CASCADE

# COMMAND ----------

#%sql
#CREATE database f1_processed 

# COMMAND ----------


