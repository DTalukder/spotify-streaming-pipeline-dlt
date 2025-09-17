# Databricks notebook source
# /00_config.py

# ---------------- WIDGETS ----------------
dbutils.widgets.dropdown("init_load_flag", "0", ["0", "1"], "Init Load?")
dbutils.widgets.text("catalog",        "spot_cat")            # my catalog
dbutils.widgets.text("schema_bronze",   "bronze")
dbutils.widgets.text("schema_silver",   "silver")
dbutils.widgets.text("schema_gold",     "gold")
dbutils.widgets.text("storage_account", "spotstream")          # my storage account
dbutils.widgets.text("container_source","source")
dbutils.widgets.text("container_bronze","bronze")
dbutils.widgets.text("container_silver","silver")
dbutils.widgets.text("container_gold",  "gold")

# ---------------- READ WIDGETS ----------------
INIT = int(dbutils.widgets.get("init_load_flag"))
CAT  = dbutils.widgets.get("catalog").strip()
BZ   = dbutils.widgets.get("schema_bronze").strip()
SV   = dbutils.widgets.get("schema_silver").strip()
GD   = dbutils.widgets.get("schema_gold").strip()
SA   = dbutils.widgets.get("storage_account").strip()
SRC  = dbutils.widgets.get("container_source").strip()
BRZC = dbutils.widgets.get("container_bronze").strip()
SILC = dbutils.widgets.get("container_silver").strip()
GLDC = dbutils.widgets.get("container_gold").strip()

# ---------------- HELPERS ----------------
def abfss(container: str, path: str = ""):
    base = f"abfss://{container}@{SA}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

def qualify(schema: str, table: str) -> str:
    return f"{CAT}.{schema}.{table}" if CAT else f"{schema}.{table}"

# ---------------- DLT Paths ----------------
SCHEMA_BASE = abfss(BRZC, "_schemas")
CHK_BASE    = abfss(BRZC, "_checkpoints")

# ---------------- LAYER ROOTS ----------------
PATH_SRC = abfss(SRC)
PATH_BZ  = abfss(BRZC)
PATH_SV  = abfss(SILC)
PATH_GD  = abfss(GLDC)

# ---------------- IMPORTS & CATALOG ----------------
from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window as W

try:
    cur_cat = spark.sql("select current_catalog()").first()[0]
    if not cur_cat:
        CAT = ""
except Exception:
    CAT = ""

if CAT:
    spark.sql(f"USE CATALOG {CAT}")

# ---------------- LOG ----------------
assert SA and "<your_sa>" not in SA, "Set storage_account to your ADLS name."
print(f"CONFIG :: CATALOG={CAT or 'hive_metastore'} | SA={SA}")
print(f"PATHS  :: SRC={PATH_SRC} | BZ={PATH_BZ} | SV={PATH_SV} | GD={PATH_GD}")