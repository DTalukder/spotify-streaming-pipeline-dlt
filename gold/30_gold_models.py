# Databricks notebook source
# MAGIC %md
# MAGIC ## **Objective**
# MAGIC - Aggregate key metrics
# MAGIC - Group and rank by business logic
# MAGIC - Create time-based trends
# MAGIC - Apply window functions
# MAGIC - Add a transformation timestamp
# MAGIC - Create modular Gold outputs (top songs, yearly, trending)
# MAGIC - Write final Delta Tables to Gold path
# MAGIC - Validate Gold outputs

# COMMAND ----------

# MAGIC %md
# MAGIC **Run config**

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data**

# COMMAND ----------

dbutils.widgets.text("file_name", "streaming")
file_name = dbutils.widgets.get("file_name").strip()


silver_path = f"{PATH_SV}/{file_name}"
gold_base = f"{PATH_GD}/{file_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Silver table**

# COMMAND ----------

silver_df = spark.read.format("delta").load(silver_path)

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **MODEL 1 - Top 10 Songs by Total Streams**

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, round, desc

model_1_df = (
    silver_df
    .filter(col("total_streams_millions").isNotNull())
    .groupBy("album", "artist")
    .agg({"total_streams_millions": "sum"})
    .withColumnRenamed("sum(total_streams_millions)", "total_streams_millions")
    .orderBy(desc("total_streams_millions"))
    .limit(10)
)

# COMMAND ----------

model_1_df = model_1_df.withColumn("total_streams_millions", round("total_streams_millions", 2))

# COMMAND ----------

model_1_df = model_1_df.withColumn("gold_model_time", current_timestamp())

# COMMAND ----------

display(model_1_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data**

# COMMAND ----------

target_path = f"{PATH_GD}/top_songs_by_streams"

# try/except to prevent crash if path doesn't exist
try:
    existing_files = dbutils.fs.ls(target_path)
    already_exists = any(f.name.startswith("part-") for f in existing_files)
except Exception:
    already_exists = False  # if path doesn't exist, write one

if not already_exists:
    model_1_df.write.format("delta").mode("overwrite").save(target_path)
    print("Model 1 written to:", target_path)
else:
    print("Skipped: Model 1 already exists at:", target_path)


# COMMAND ----------

# MAGIC %md
# MAGIC ### **Model 2 - Top Artist per year**

# COMMAND ----------

from pyspark.sql.functions import year, sum as _sum, row_number
from pyspark.sql.window import Window as W

window_year = W.partitionBy("release_year").orderBy(desc("streams_last_30_days_millions"))


model_2_df = (
silver_df
.filter(col("streams_last_30_days_millions").isNotNull())
.withColumn("rank_in_year", row_number().over(window_year))
.filter(col("rank_in_year") <= 5) # Top 5 per year
.withColumn("gold_model_time", current_timestamp())
)

# COMMAND ----------

display(model_2_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data**

# COMMAND ----------

target_path = f"{PATH_GD}/top_streams_by_year"

# try/except to prevent crash if path doesn't exist
try:
    existing_files = dbutils.fs.ls(target_path)
    already_exists = any(f.name.startswith("part-") for f in existing_files)
except Exception:
    already_exists = False  # if path doesn't exist, write one

if not already_exists:
    model_2_df.write.format("delta").mode("overwrite").save(target_path)
    print("Model 2 written to:", target_path)
else:
    print("Skipped: Model 2 already exists at:", target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Model 3 - Average stream duration by genre**

# COMMAND ----------

from pyspark.sql.functions import avg, round, current_timestamp

model_3_df = (
silver_df
.groupBy("genre")
.agg(round(avg("avg_stream_duration_min"), 2).alias("avg_duration_min"))
.orderBy(desc("avg_duration_min"))
.withColumn("gold_model_time", current_timestamp())
)

# COMMAND ----------

display(model_3_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data**

# COMMAND ----------

target_path = f"{PATH_GD}/avg_duration_by_genre"

# try/except to prevent crash if path doesn't exist
try:
    existing_files = dbutils.fs.ls(target_path)
    already_exists = any(f.name.startswith("part-") for f in existing_files)
except Exception:
    already_exists = False  # if path doesn't exist, write one

if not already_exists:
    model_3_df.write.format("delta").mode("overwrite").save(target_path)
    print("Model 3 written to:", target_path)
else:
    print("Skipped: Model 3 already exists at:", target_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Model 4 - Country-level streaming trends**

# COMMAND ----------

model_4_df = (
silver_df
.groupBy("country")
.agg(
round(_sum("streams_last_30_days_millions"), 2).alias("total_streams_30d"),
round(avg("skip_rate_%"), 2).alias("avg_skip_rate")
)
.orderBy(desc("total_streams_30d"))
.withColumn("gold_model_time", current_timestamp())
)

# COMMAND ----------

display(model_4_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Write data**

# COMMAND ----------

target_path = f"{PATH_GD}/genre_country_platform"

# try/except to prevent crash if path doesn't exist
try:
    existing_files = dbutils.fs.ls(target_path)
    already_exists = any(f.name.startswith("part-") for f in existing_files)
except Exception:
    already_exists = False  # if path doesn't exist, write one

if not already_exists:
    model_4_df.write.format("delta").mode("overwrite").save(target_path)
    print("Model 4 written to:", target_path)
else:
    print("Skipped: Model 4 already exists at:", target_path)