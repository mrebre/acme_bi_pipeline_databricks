# Databricks notebook source
# MAGIC %md
# MAGIC # ⚙️ Setup — Schema Creation (Run Once) and file uppload
# MAGIC This notebook should be executed ONCE before all other scripts.

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload the file 
# MAGIC ```
# MAGIC ## Uppload file data/Case Study BI.xlsx to  location: /Volumes/workspace/acme_files/uploads/Case Study BI.xlsx   ← Run once at the beginning
# MAGIC ```

# workspace. prefix is mandatory in Databricks Unity Catalog
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

print("Schemas ready: workspace.bronze, workspace.silver, workspace.gold")


