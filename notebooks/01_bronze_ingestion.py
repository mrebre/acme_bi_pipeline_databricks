# Databricks notebook source
# MAGIC %md
# MAGIC # 🥉 Bronze Layer — Raw Data Ingestion
# MAGIC Ingesting raw source data from Excel spreadsheets and saving them as foundational Delta tables without any structural modifications.

# COMMAND ----------

# Install openpyxl for reading xlsx files
from pyspark.sql import SparkSession
import pandas as pd
%pip install openpyxl   # <- uncomment first time

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# File pathway inside the Unity Catalog Volume.
# 1. Create Volume: CREATE VOLUME main.bronze.landing_zone;
# 2. Upload the source file to that specific Volume.
FILE_PATH = "/Volumes/workspace/acme_files/uploads/Case Study BI.xlsx"

# Map: Target table name -> Source Excel sheet name
SHEET_MAP = {
    "raw_categories":    "Sheet2",
    "raw_customers":     "Sheet3",
    "raw_divisions":     "Sheet4",
    "raw_order_details": "Sheet5",
    "raw_orders":        "Sheet6",
    "raw_products":      "Sheet7",
    "raw_shipments":     "Sheet8",
    "raw_shippers":      "Sheet9",
}

# Ingest each sheet and save it as a Bronze Delta table
for table_name, sheet_name in SHEET_MAP.items():

    pdf = pd.read_excel(FILE_PATH, sheet_name=sheet_name)

    # Convert to Spark DataFrame and persist as Delta format
    sdf = spark.createDataFrame(pdf)
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze.{table_name}"))

    print(f"✅ {table_name}: {sdf.count()} rows")

print("\nBronze layer ingestion complete.")