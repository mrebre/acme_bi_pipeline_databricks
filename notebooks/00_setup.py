# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Setup — Schema Creation (Run Once) and File Upload
# MAGIC This notebook should be executed ONCE before all other scripts.

# COMMAND ----------

# workspace. prefix is mandatory in Databricks Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

print("Schemas ready: workspace.bronze, workspace.silver, workspace.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 📥 1. SOURCE FILE INGESTION (Manual Action Required)
# MAGIC
# MAGIC To ensure the pipeline runs correctly, the source Excel file must be uploaded **once** to the designated Databricks Unity Catalog Volume location.
# MAGIC
# MAGIC * **Source File:** `data/Case Study BI.xlsx`
# MAGIC * **Target Location (Volume):** `/Volumes/workspace/acme_files/uploads/Case Study BI.xlsx`
# MAGIC
# MAGIC > ⚠️ **Note:** If the `uploads` directory does not exist within your Volume, please create it manually through the Databricks Catalog UI before dropping the file.
