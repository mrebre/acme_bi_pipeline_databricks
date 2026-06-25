# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Setup — Schema Creation (Run Once) and file uppload
# MAGIC This notebook should be executed ONCE before all other scripts.

# COMMAND ----------

# workspace. prefix is mandatory in Databricks Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

print("Schemas ready: workspace.bronze, workspace.silver, workspace.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execution Order
# MAGIC ```
# MAGIC Uppload file "Case Study BI.xlsx" to this location: /Volumes/workspace/acme_files/uploads/Case Study BI.xlsx"  ← Run once at the beginning     ← Run once at the beginning
# MAGIC 00_setup.py              ← Run once at the beginning
# MAGIC 01_bronze_ingestion.py   ← Loads Excel data into Delta format
# MAGIC 02_bronze_profiling.py   ← Profiles raw incoming data
# MAGIC 03_silver_cleansing.py   ← Handles cleansing and Unknown Shipper mapping
# MAGIC 04_silver_star_schema.py ← Compiles Star Schema + Enforces DQ Gate
# MAGIC 05_gold_kpis.sql         ← Materializes KPI views for BI consumption
# MAGIC ```
