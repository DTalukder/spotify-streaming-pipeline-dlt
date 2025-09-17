-- Databricks notebook source
-- 1. Row counts across layers
SELECT
  (SELECT COUNT(*) FROM spot_cat.spotify_bronze.bronze_spotify_raw) AS bronze_rows,
  (SELECT COUNT(*) FROM spot_cat.spotify_silver.silver_spotify_cleaned) AS silver_rows,
  (SELECT COUNT(*) FROM spot_cat.spotify_gold.gold_yearly_stream_trends) AS gold_rows_yearly_stream_trends,
  (SELECT COUNT(*) FROM spot_cat.spotify_gold.gold_top_songs_per_year) AS gold_rows_top_songs_per_year,
  (SELECT COUNT(*) FROM spot_cat.spotify_gold.gold_top_artists) AS gold_rows_top_artists
;

-- COMMAND ----------

-- 2. Null checks for key columns
SELECT
  (SELECT COUNT(*) FROM spot_cat.spotify_bronze.bronze_spotify_raw WHERE Artist IS NULL) AS null_artists_bronze,
  (SELECT COUNT(*) FROM spot_cat.spotify_silver.silver_spotify_cleaned WHERE Streams_Last_30_Days_Millions IS NULL) AS null_streams_silver
;

-- COMMAND ----------

-- 3. Distinct validation
SELECT
  (SELECT COUNT(DISTINCT Artist) FROM spot_cat.spotify_silver.silver_spotify_cleaned) AS distinct_artists_silver,
  (SELECT COUNT(DISTINCT Artist) FROM spot_cat.spotify_gold.gold_top_artists) AS distinct_artists_gold
;