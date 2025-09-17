# Databricks notebook source
# MAGIC %md
# MAGIC ## **Objective**
# MAGIC - Drop metadata columns
# MAGIC - Standarize column names
# MAGIC - Correct data types
# MAGIC - Filter nulls (if needed)
# MAGIC - Write silver/streaming path
# MAGIC - Validate silver layer

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC **Read data**

# COMMAND ----------

dbutils.widgets.text("file_name", "streaming")
file_name = dbutils.widgets.get("file_name").strip()

# COMMAND ----------

bronze_path = f"{PATH_BZ}/{file_name}"
silver_path = f"{PATH_SV}/{file_name}"

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Bronze table**

# COMMAND ----------

bronze_df = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC **Clean columns**

# COMMAND ----------

for col_name in bronze_df.columns:
    clean_col = col_name.strip().replace(" ", "_").replace(", ", "_").replace("(", "").replace(")", "")
    bronze_df = bronze_df.withColumnRenamed(col_name, clean_col)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Drop columns**

# COMMAND ----------

drop_cols = ["_rescued_data", "source_file"]
existing_cols = [c for c in drop_cols if c in bronze_df.columns]
bronze_df = bronze_df.drop(*existing_cols)

# COMMAND ----------

display(bronze_df.limit(20))

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC **Deduplicate**

# COMMAND ----------

silver_df = bronze_df.dropDuplicates()

# COMMAND ----------

display(silver_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformation timestamp**

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
silver_df = silver_df.withColumn("silver_transform_time", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **Write to Silver**

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").save(silver_path)

# COMMAND ----------

# Validate

display(spark.read.format("delta").load(silver_path))