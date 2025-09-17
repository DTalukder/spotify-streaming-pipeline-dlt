# Databricks notebook source
# MAGIC %md
# MAGIC **Run config**

# COMMAND ----------

# MAGIC %run ./00_config  

# COMMAND ----------

print(f"PATH_SRC = {PATH_SRC}")
print(f"SRC (container) = {SRC}")


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Read the raw data**

# COMMAND ----------

# raw_df = spark.read.format("csv") \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .load(f"{PATH_SRC}/streaming/Spotify_2024_Global_Streaming_Data.csv")

# raw_df.display()

# #Won't work after chunking completes. Original raw file will be deleted.

# COMMAND ----------

display(dbutils.fs.ls(f"{PATH_SRC}/streaming/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Chunk the files into 5 pieces**

# COMMAND ----------

# from pyspark.sql.functions import monotonically_increasing_id, col
# from pyspark.sql import Row

# # Add row index
# indexed_df = raw_df.withColumn("row_id", monotonically_increasing_id())

# # Count and compute chunk size
# row_count = indexed_df.count()
# chunk_size = row_count // 5

# # Split main raw file and write
# for i in range(5):
#     start = i * chunk_size
#     end = row_count if i == 4 else (i + 1) * chunk_size

#     chunk_df = indexed_df.filter((col("row_id") >= start) & (col("row_id") < end)).drop("row_id")

#     temp_path = f"/tmp/chunk_{i+1}"
#     chunk_df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

#     part_file = [f.path for f in dbutils.fs.ls(temp_path) if f.name.startswith("part-")][0]
#     final_path = f"{PATH_SRC}/streaming/chunk_{i+1}.csv"

#     # this should work now
#     dbutils.fs.rm(final_path) 
#     dbutils.fs.mv(part_file, final_path)

#     dbutils.fs.rm(temp_path, recurse=True)

#     print(f"chunk_{i+1}.csv written cleanly.")

# print("All chunks saved as flat CSVs in /streaming/")


# COMMAND ----------

# MAGIC %md
# MAGIC **File targe**

# COMMAND ----------

# Define logical dataset name using Widget
dbutils.widgets.text("file_name", "streaming")
file_name = dbutils.widgets.get("file_name").strip()

# Paths from config
src_path    = abfss(SRC, file_name)            
bronze_path = abfss(BRZC, file_name)          
schema_path = f"{SCHEMA_BASE}/{file_name}"     
chk_path    = f"{CHK_BASE}/{file_name}_v1"   

print(f"SRC: {src_path}")
print(f"BRONZE: {bronze_path}")
print(f"SCHEMA: {schema_path}")
print(f"CHECKPOINT: {chk_path}")


# COMMAND ----------

# MAGIC %md
# MAGIC **Timestamp Tracking**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name

df_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.inferColumntypes", "true")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemalocation", schema_path)
    .option("recursiveFileLookup", "false")
    .option("pathGlobFilter", "*.csv")
    .load(src_path)
    .withColumn("ingesstion_time", current_timestamp())
    .withColumn("source_file", F.col("_metadata.file_path")) 
)

# COMMAND ----------

# Clean column names: replace spaces, parentheses, etc.
for col_name in df_stream.columns:
    clean_col = col_name.strip().lower().replace(" ", "_").replace("(", "").replace(")", "")
    df_stream = df_stream.withColumnRenamed(col_name, clean_col)


# COMMAND ----------

# MAGIC %md
# MAGIC **Write to Delta (Bronze Layer)**

# COMMAND ----------

q = (
    df_stream.writeStream.format("delta")
    .option("checkpointLocation", chk_path)
    .option("path", bronze_path)
    .outputMode("append")
    .trigger(once=True)
    .start()
)

q.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC **Validate Bronze Layer**

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)
display(bronze_df.orderBy(F.desc("ingesstion_time")))

# COMMAND ----------

bronze_df.count()