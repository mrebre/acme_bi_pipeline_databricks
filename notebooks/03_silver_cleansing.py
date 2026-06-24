# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 03 — Silver Cleansing
# MAGIC
# MAGIC **Input:** `workspace.bronze.*`
# MAGIC **Output:** `workspace.silver.clean_*`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Load All Bronze Tables

# COMMAND ----------

raw_orders = spark.table("workspace.bronze.raw_orders")
raw_order_details = spark.table("workspace.bronze.raw_order_details")
raw_customers = spark.table("workspace.bronze.raw_customers")
raw_products = spark.table("workspace.bronze.raw_products")
raw_categories = spark.table("workspace.bronze.raw_categories")
raw_divisions = spark.table("workspace.bronze.raw_divisions")
raw_shippers = spark.table("workspace.bronze.raw_shippers")
raw_shipments = spark.table("workspace.bronze.raw_shipments")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. empty_to_null — Excel empty cells arrive as "" instead of actual programmatic NULLs

# COMMAND ----------

def empty_to_null(df):
    """Convert empty/whitespace strings into programmatic NULL fields. Evaluates string datatypes only."""
    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(F.trim(F.col(field.name)) == "", None)
                 .otherwise(F.col(field.name))
            )
    return df


clean_orders = empty_to_null(raw_orders)
clean_order_details = empty_to_null(raw_order_details)
clean_customers = empty_to_null(raw_customers)
clean_products = empty_to_null(raw_products)
clean_categories = empty_to_null(raw_categories)
clean_divisions = empty_to_null(raw_divisions)
clean_shippers = empty_to_null(raw_shippers)
clean_shipments = empty_to_null(raw_shipments)

print("empty_to_null applied ✅")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Unknown Shipper — Resolving Critical Foreign Key Violations
# MAGIC
# MAGIC ShipperID records 4 and 5 exist within orders and shipments transactions but are absent from the shippers lookup table.
# MAGIC This anomaly impacts ~35% of all operational entries — we cannot drop them from production.
# MAGIC We resolve this by appending a structural fallback placeholder row to ensure downstream LEFT JOIN executions don't return NULL strings.
# MAGIC
# MAGIC Production Note: Establish client communication to identify the root cause of missing upstream values.

# COMMAND ----------

# Isolate all missing ShipperID keys from transactional tables
missing_shipper_ids = (
    clean_shipments.select("ShipperID").distinct()
    .union(clean_orders.select("ShipperID").distinct())
    .distinct()
    .join(clean_shippers.select("ShipperID"), on="ShipperID", how="left_anti")
    .orderBy("ShipperID")
)

print("Missing ShipperID values isolated (to be generated as Unknown):")
missing_shipper_ids.show()

# Construct fallback placeholder rows
unknown_rows = missing_shipper_ids.withColumn(
    "CompanyName", F.lit("Unknown Shipper"))

# Match schema layout requirements by filling empty lookup slots with null literals
for field in clean_shippers.schema.fields:
    if field.name not in ["ShipperID", "CompanyName"]:
        unknown_rows = unknown_rows.withColumn(
            field.name, F.lit(None).cast(field.dataType))

# Merge valid master records with generated placeholder bounds
clean_shippers = clean_shippers.unionByName(
    unknown_rows, allowMissingColumns=True)

print("Shippers lookup state following Unknown placeholder insertion:")
clean_shippers.orderBy("ShipperID").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. FK Verification — All Foreign Key references must be checked prior to modeling transition

# COMMAND ----------

fk_checks = {
    "ShipperID: orders → shippers": (
        clean_orders.select("ShipperID").distinct()
        .join(clean_shippers.select("ShipperID"), on="ShipperID", how="left_anti").count()
    ),
    "ShipperID: shipments → shippers": (
        clean_shipments.select("ShipperID").distinct()
        .join(clean_shippers.select("ShipperID"), on="ShipperID", how="left_anti").count()
    ),
    "CustomerID: orders → customers": (
        clean_orders.select("CustomerID").distinct()
        .join(clean_customers.select("CustomerID"), on="CustomerID", how="left_anti").count()
    ),
    "ProductID: order_details → products": (
        clean_order_details.select("ProductID").distinct()
        .join(clean_products.select("ProductID"), on="ProductID", how="left_anti").count()
    ),
    "DivisionID: customers → divisions": (
        clean_customers.select("DivisionID").distinct()
        .join(clean_divisions.select("DivisionID"), on="DivisionID", how="left_anti").count()
    ),
    "CategoryID: products → categories": (
        clean_products.select("CategoryID").distinct()
        .join(clean_categories.select("CategoryID"), on="CategoryID", how="left_anti").count()
    ),
}

all_ok = True
print("FK Verification Audit Summary:")
print("=" * 55)
for check, count in fk_checks.items():
    status = "✅ OK" if count == 0 else f"⚠️  {count} orphaned records isolated!"
    if count > 0:
        all_ok = False
    print(f"  {check:<45} {status}")

if not all_ok:
    raise Exception("Critical FK constraints broken — Halting pipeline orchestration flow!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Persist Cleaned Silver Layer Data Structures

# COMMAND ----------

clean_tables = {
    "clean_orders":        clean_orders,
    "clean_order_details": clean_order_details,
    "clean_customers":     clean_customers,
    "clean_products":      clean_products,
    "clean_categories":    clean_categories,
    "clean_divisions":     clean_divisions,
    "clean_shippers":      clean_shippers,
    "clean_shipments":     clean_shipments,
}

for name, df in clean_tables.items():
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"workspace.silver.{name}")
    print(f"✅ workspace.silver.{name}: {df.count():,} rows")

print("\n✅ Cleansing complete")