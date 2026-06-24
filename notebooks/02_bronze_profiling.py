# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ Automated Data Quality Gate — Bronze Layer
# MAGIC Comprehensive and automated integrity auditing of the entire relational model before establishing the Silver layer tier.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Initialization and Registration of all Bronze Tables

# COMMAND ----------
tables = {
    "raw_orders":        spark.table("workspace.bronze.raw_orders"),
    "raw_order_details": spark.table("workspace.bronze.raw_order_details"),
    "raw_customers":     spark.table("workspace.bronze.raw_customers"),
    "raw_products":      spark.table("workspace.bronze.raw_products"),
    "raw_categories":    spark.table("workspace.bronze.raw_categories"),
    "raw_divisions":     spark.table("workspace.bronze.raw_divisions"),
    "raw_shippers":      spark.table("workspace.bronze.raw_shippers"),
    "raw_shipments":     spark.table("workspace.bronze.raw_shipments")
}

print("=" * 55)
print(f"{'Table Name':<25} {'Rows':>8} {'Columns':>8}")
print("=" * 55)
for name, df in tables.items():
    print(f"{name:<25} {df.count():>8,} {len(df.columns):>8}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Automated NULL & Empty String Scanner (All tables, all columns scanned in a single pass)

# COMMAND ----------

def optimized_null_report(df, table_name):
    """Computes all NULL and empty spaces in a single optimized pass through the table data."""
    total = df.count()
    if total == 0:
        return

    agg_exprs = []
    for field in df.schema.fields:
        c = field.name
        if isinstance(field.dataType, StringType):
            condition = F.col(c).isNull() | (F.trim(F.col(c)) == "")
        else:
            condition = F.col(c).isNull()
        agg_exprs.append(F.sum(F.when(condition, 1).otherwise(0)).alias(c))

    result_row = df.agg(*agg_exprs).collect()[0].asDict()

    print(f"\n📊 Table: {table_name} (Total Rows: {total:,})")
    print("-" * 55)
    for col_name, null_count in result_row.items():
        pct = (null_count / total * 100)
        flag = " ⚠️" if pct > 0 else " ✅"
        print(f"  -> {col_name:<25} {null_count:>8,} ({pct:>5.1f}%){flag}")


print("=" * 65)
print("🚀 LAUNCHING GLOBAL NULL / EMPTY STRING SCANNER")
print("=" * 65)
for name, df in tables.items():
    optimized_null_report(df, name)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Global Uniqueness Verification of Primary Keys and Row Duplicates

# COMMAND ----------
pk_mapping = {
    "raw_orders":        ["OrderID"],
    "raw_order_details": ["OrderID", "LineNo"],
    "raw_customers":     ["CustomerID"],
    "raw_products":      ["ProductID"],
    "raw_categories":    ["CategoryID"],
    "raw_divisions":     ["DivisionID"],
    "raw_shippers":      ["ShipperID"],
    "raw_shipments":     ["OrderID", "LineNo"]
}

print("=" * 65)
print("🎯 PRIMARY KEY UNIQUENESS AND ROW INTEGRITY CHECK")
print("=" * 65)

for table_name, pk_cols in pk_mapping.items():
    df = tables[table_name]
    total_rows = df.count()

    # 1. Check for pure record duplicates (identical entire rows)
    distinct_rows = df.distinct().count()
    row_dupes = total_rows - distinct_rows

    # 2. Check for core primary key uniqueness boundaries
    distinct_pks = df.select(pk_cols).distinct().count()
    pk_dupes = total_rows - distinct_pks

    status = "✅ OK"
    if row_dupes > 0 or pk_dupes > 0:
        status = f"⚠️ ANOMALY DETECTED! (Row Dupes: {row_dupes}, PK Dupes: {pk_dupes})"

    print(f"  {table_name:<25} -> {status}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. Automated Referential Integrity Check (All Foreign Keys Scanner)

# COMMAND ----------
fk_relationships = [
    ("raw_order_details", "OrderID",    "raw_orders",     "OrderID"),
    ("raw_order_details", "ProductID",  "raw_products",   "ProductID"),
    ("raw_orders",        "CustomerID", "raw_customers",  "CustomerID"),
    ("raw_orders",        "ShipperID",  "raw_shippers",   "ShipperID"),
    ("raw_products",      "CategoryID", "raw_categories", "CategoryID"),
    ("raw_customers",     "DivisionID", "raw_divisions",  "DivisionID"),
    ("raw_shipments",     "OrderID",    "raw_orders",     "OrderID"),
    ("raw_shipments",     "ShipperID",  "raw_shippers",   "ShipperID"),
    ("raw_shipments",     "CustomerID", "raw_customers",  "CustomerID"),
    ("raw_shipments",     "ProductID",  "raw_products",   "ProductID")
]

print("=" * 65)
print("🔗 REFERENTIAL INTEGRITY SCANNING (FOREIGN KEY VALIDATIONS)")
print("=" * 65)

for fk_table, fk_col, pk_table, pk_col in fk_relationships:
    df_fk = tables[fk_table].select(fk_col).filter(
        F.col(fk_col).isNotNull()).distinct()
    df_pk = tables[pk_table].select(F.col(pk_col).alias(fk_col)).distinct()

    # Locate orphaned records using a Left Anti Join
    orphans = df_fk.join(df_pk, on=fk_col, how="left_anti")
    orphan_count = orphans.count()

    if orphan_count > 0:
        orphan_list = [str(row[0]) for row in orphans.limit(5).collect()]
        print(f"❌ ERROR: {fk_table}.{fk_col} -> {pk_table}.{pk_col}")
        print(
            f"   -> Found {orphan_count} non-existent keys! Examples: {', '.join(orphan_list)}")
    else:
        print(
            f"✅ RELATION VALID: {fk_table}.{fk_col} -> {pk_table}.{pk_col}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. Comprehensive Logical and Business Validation (Value Distribution & Outlier Detection)
# MAGIC Granular engineering scanning of critical columns across all tables to catch extreme, invalid, or illogical entries.

# COMMAND ----------
print("=" * 70)
print("🧠 LAUNCHING GLOBAL BUSINESS LOGIC AND OUTLIER DETECTOR")
print("=" * 70)

# --- 5a. TABLE: raw_orders ---
print("\n[Validation: raw_orders]")
print("-" * 50)
orders_check = tables["raw_orders"].agg(
    F.sum(F.when(F.col("OrderID") <= 0, 1).otherwise(0)).alias("Invalid_OrderID"),
    F.sum(F.when(F.col("EmployeeID") <= 0, 1).otherwise(
        0)).alias("Invalid_EmployeeID"),
    F.sum(F.when(F.col("Freight") < 0, 1).otherwise(
        0)).alias("Negative_Freight"),
    F.max("Freight").alias("Max_Freight_Outlier"),
    F.min("OrderDate").alias("Min_OrderDate"),
    F.max("OrderDate").alias("Max_OrderDate"),
    F.sum(F.when((F.year("OrderDate") < 2000) | (F.year("OrderDate")
          > 2030), 1).otherwise(0)).alias("Impossible_Dates")
).collect()[0].asDict()

for rule, val in orders_check.items():
    print(f"  -> {rule:<25}: {val}")


# --- 5b. TABLE: raw_order_details ---
print("\n[Validation: raw_order_details]")
print("-" * 50)
details_check = tables["raw_order_details"].agg(
    F.sum(F.when(F.col("OrderID") <= 0, 1).otherwise(0)).alias("Invalid_OrderID"),
    F.sum(F.when(F.col("LineNo") <= 0, 1).otherwise(0)).alias("Invalid_LineNo"),
    F.sum(F.when(F.col("ProductID") <= 0, 1).otherwise(
        0)).alias("Invalid_ProductID"),
    F.sum(F.when(F.col("Quantity") <= 0, 1).otherwise(
        0)).alias("Negative_Quantity"),
    F.sum(F.when(F.col("UnitPrice") <= 0, 1).otherwise(
        0)).alias("Negative_Price"),
    F.sum(F.when((F.col("Discount") < 0) | (F.col("Discount") > 1),
          1).otherwise(0)).alias("Invalid_Discount"),
    F.max("Quantity").alias("Max_Quantity"),
    F.max("UnitPrice").alias("Max_UnitPrice")
).collect()[0].asDict()

for rule, val in details_check.items():
    print(f"  -> {rule:<25}: {val}")


# --- 5c. TABLE: raw_products ---
print("\n[Validation: raw_products]")
print("-" * 50)
products_check = tables["raw_products"].agg(
    F.sum(F.when(F.col("ProductID") <= 0, 1).otherwise(
        0)).alias("Invalid_ProductID"),
    F.sum(F.when(F.col("SupplierID") <= 0, 1).otherwise(
        0)).alias("Invalid_SupplierID"),
    F.sum(F.when(F.col("CategoryID") <= 0, 1).otherwise(
        0)).alias("Invalid_CategoryID"),
    F.sum(F.when(F.col("UnitCost") <= 0, 1).otherwise(
        0)).alias("Negative_UnitCost"),
    F.sum(F.when(F.col("UnitPrice") <= 0, 1).otherwise(
        0)).alias("Negative_UnitPrice"),
    F.sum(F.when(F.col("UnitsInStock") < 0, 1).otherwise(
        0)).alias("Negative_UnitsInStock"),
    F.sum(F.when(F.col("UnitsOnOrder") < 0, 1).otherwise(
        0)).alias("Negative_UnitsOnOrder"),
    F.sum(F.when(F.col("UnitCost") >= F.col("UnitPrice"), 1).otherwise(
        0)).alias("Products_With_Negative_Margins"),
    F.max(F.length(F.col("ProductName"))).alias("Max_ProductName_Length")
).collect()[0].asDict()

for rule, val in products_check.items():
    print(f"  -> {rule:<25}: {val}")


# --- 5d. TABLE: raw_customers ---
print("\n[Validation: raw_customers]")
print("-" * 50)
customers_check = tables["raw_customers"].agg(
    F.sum(F.when(F.col("CustomerID") <= 0, 1).otherwise(
        0)).alias("Invalid_CustomerID"),
    F.sum(F.when(F.col("DivisionID") <= 0, 1).otherwise(
        0)).alias("Invalid_DivisionID"),
    F.max(F.length(F.col("CompanyName"))).alias("Max_CompanyName_Length"),
    F.min(F.length(F.trim(F.col("PostalCode")))).alias("Min_PostalCode_Chars"),
    F.max(F.length(F.trim(F.col("PostalCode")))).alias("Max_PostalCode_Chars")
).collect()[0].asDict()

for rule, val in customers_check.items():
    print(f"  -> {rule:<25}: {val}")


# --- 5e. TABLE: raw_shipments (Timeline and Chronological Logic) ---
print("\n[Validation: raw_shipments]")
print("-" * 50)
shipments_check = tables["raw_shipments"].agg(
    F.sum(F.when(F.col("OrderID") <= 0, 1).otherwise(0)).alias("Invalid_OrderID"),
    F.sum(F.when(F.col("LineNo") <= 0, 1).otherwise(0)).alias("Invalid_LineNo"),
    F.min("ShipmentDate").alias("Min_ShipmentDate"),
    F.max("ShipmentDate").alias("Max_ShipmentDate")
).collect()[0].asDict()

for rule, val in shipments_check.items():
    print(f"  -> {rule:<25}: {val}")

# Check for anomalies where ShipmentDate occurs before OrderDate
invalid_timeline = (
    tables["raw_shipments"].select("OrderID", "ShipmentDate")
    .join(tables["raw_orders"].select("OrderID", "OrderDate"), on="OrderID", how="inner")
    .filter(F.datediff("ShipmentDate", "OrderDate") < 0)
).count()
print(
    f"  -> Shipments recorded before order initialization (ShipmentDate < OrderDate): {invalid_timeline}")


# --- 5f. TABLES: Master Reference Data (raw_categories, raw_divisions, raw_shippers) ---
print("\n[Validation: Master References (Structural metadata integrity)]")
print("-" * 50)
for m_name in ["raw_categories", "raw_divisions", "raw_shippers"]:
    df_m = tables[m_name]
    id_col = df_m.columns[0]  # First column maps to the primary reference identifier ID
    name_col = df_m.columns[1]  # Second column maps to the structural descriptor text string

    m_check = df_m.agg(
        F.sum(F.when(F.col(id_col) <= 0, 1).otherwise(0)).alias("Invalid_ID"),
        F.min(F.length(F.trim(F.col(name_col)))).alias("Min_Name_Length"),
        F.max(F.length(F.trim(F.col(name_col)))).alias("Max_Name_Length")
    ).collect()[0].asDict()

    print(
        f"  -> {m_name} | Invalid ID: {m_check['Invalid_ID']} | Name character bounds (Min: {m_check['Min_Name_Length']}, Max: {m_check['Max_Name_Length']})")