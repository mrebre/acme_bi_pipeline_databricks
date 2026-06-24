# Databricks notebook source
# MAGIC %md
# MAGIC # 📐 04 — Silver Star Schema
# MAGIC **Default Language:** Python
# MAGIC **Input:** `workspace.silver.clean_*`
# MAGIC **Output:** `workspace.silver.dim_*`, `workspace.silver.fact_sales`
# MAGIC
# MAGIC Leverages fast declarative SQL constructs for dimensional construction and table building,
# MAGIC followed by Python validation logic acting as a programmatic Data Quality Gate to manage pipeline execution.
# MAGIC
# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_products

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Products joined with descriptive classification structures (ProductGroup) and operational tracking costs (UnitCost)
# MAGIC -- UnitCost parameter is required inside fact_sales to generate accurate downstream COGS metrics
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_products AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY p.ProductID) AS ProductKey,
# MAGIC     p.ProductID,
# MAGIC     p.ProductName,
# MAGIC     c.CategoryName                                              AS ProductGroup,
# MAGIC     c.Description,
# MAGIC     p.UnitCost,
# MAGIC     p.UnitPrice                                                 AS ListPrice,
# MAGIC     -- Baseline margin structure calculated via catalog values prior to active discounting — for benchmarking
# MAGIC     ROUND((p.UnitPrice - p.UnitCost) / p.UnitPrice * 100, 2)   AS BaseMarginPct
# MAGIC FROM workspace.silver.clean_products p
# MAGIC LEFT JOIN workspace.silver.clean_categories c ON p.CategoryID = c.CategoryID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_geography
# MAGIC
# MAGIC **Structural Hotfix:** The structural region parameter maps directly from `clean_divisions` data instead of `clean_customers`.
# MAGIC The `clean_customers` schema registers only `DivisionID` attributes — establishing an explicit relational JOIN is mandatory.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Region (Division), Country, and City built as an independent, fully conformed spatial geography dimension table.
# MAGIC -- Task requires analytical cross-sections for Region, Country, and Division elements — all grouped here.
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_geography AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY d.DivisionName, c.Country, c.City) AS GeographyKey,
# MAGIC     d.DivisionID,
# MAGIC     d.DivisionName  AS Region,
# MAGIC     c.Country,
# MAGIC     c.City
# MAGIC FROM (
# MAGIC     SELECT DISTINCT City, Country, DivisionID
# MAGIC     FROM workspace.silver.clean_customers
# MAGIC     WHERE City IS NOT NULL OR Country IS NOT NULL
# MAGIC ) c
# MAGIC LEFT JOIN workspace.silver.clean_divisions d ON c.DivisionID = d.DivisionID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_customers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Customers linked directly via GeographyKey structure referencing conformed dim_geography records.
# MAGIC -- DivisionID retained to support granular data lineage traceability.
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_customers AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY c.CustomerID) AS CustomerKey,
# MAGIC     c.CustomerID,
# MAGIC     c.CompanyName   AS CustomerName,
# MAGIC     c.ContactName,
# MAGIC     g.GeographyKey,
# MAGIC     c.DivisionID
# MAGIC FROM workspace.silver.clean_customers c
# MAGIC LEFT JOIN workspace.silver.dim_geography g
# MAGIC     ON COALESCE(c.City, '')    = COALESCE(g.City, '')
# MAGIC    AND COALESCE(c.Country, '') = COALESCE(g.Country, '');

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_shippers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Captures standard lookup elements alongside established fallback records (Unknown Shipper placeholder)
# MAGIC -- mapped during script 03_silver_cleansing to trace orphaned ShipperID values 4 and 5.
# MAGIC -- IsUnknown indicator configuration simplifies advanced reporting filter setups within Power BI canvases.
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_shippers AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY ShipperID)     AS ShipperKey,
# MAGIC     ShipperID,
# MAGIC     CompanyName                                 AS ShipperName,
# MAGIC     CASE WHEN CompanyName = 'Unknown Shipper'
# MAGIC          THEN TRUE ELSE FALSE END               AS IsUnknown
# MAGIC FROM workspace.silver.clean_shippers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_date
# MAGIC
# MAGIC Essential foundation structure supporting granular Power BI time-intelligence analytics (YTD, LYTD, MTD formulas).
# MAGIC Generated as an exhaustive complete calendar tracking sequence extending through bounds from minimum to maximum recorded OrderDate.
# MAGIC FiscalYear logic accounts for a standard corporate April opening boundary.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.silver.dim_date AS
# MAGIC SELECT
# MAGIC     CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT)          AS DateKey,
# MAGIC     date                                                AS Date,
# MAGIC     YEAR(date)                                          AS Year,
# MAGIC     QUARTER(date)                                       AS Quarter,
# MAGIC     CONCAT('Q', QUARTER(date))                          AS QuarterName,
# MAGIC     MONTH(date)                                         AS Month,
# MAGIC     DATE_FORMAT(date, 'MMMM')                           AS MonthName,
# MAGIC     DATE_FORMAT(date, 'MMM')                            AS MonthShort,
# MAGIC     WEEKOFYEAR(date)                                    AS WeekOfYear,
# MAGIC     DAYOFMONTH(date)                                    AS DayOfMonth,
# MAGIC     DAYOFWEEK(date)                                     AS DayOfWeek,
# MAGIC     DATE_FORMAT(date, 'EEEE')                           AS DayName,
# MAGIC     CASE WHEN DAYOFWEEK(date) IN (1, 7)
# MAGIC          THEN TRUE ELSE FALSE END                       AS IsWeekend,
# MAGIC     -- Fiscal year begins in April
# MAGIC     CASE WHEN MONTH(date) >= 4
# MAGIC          THEN YEAR(date)
# MAGIC          ELSE YEAR(date) - 1 END                        AS FiscalYear,
# MAGIC     CASE WHEN MONTH(date) >= 4
# MAGIC          THEN CEIL((MONTH(date) - 3) / 3)
# MAGIC          ELSE CEIL((MONTH(date) + 9) / 3) END           AS FiscalQuarter
# MAGIC FROM (
# MAGIC     SELECT EXPLODE(SEQUENCE(
# MAGIC         (SELECT CAST(MIN(OrderDate) AS DATE) FROM workspace.silver.clean_orders),
# MAGIC         (SELECT CAST(MAX(OrderDate) AS DATE) FROM workspace.silver.clean_orders),
# MAGIC         INTERVAL 1 DAY
# MAGIC     )) AS date
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. fact_sales
# MAGIC
# MAGIC Central transaction repository grain mapped at line-item precision level per order line entry (OrderID + LineNo keys).
# MAGIC Retains primary dimensional foreign keys and numerical metrics exclusively, decoupling descriptive context structures.
# MAGIC
# MAGIC ## Metrics Matrix:
# MAGIC | Analytical Metric | Algebraic Definition |
# MAGIC |--------|---------|
# MAGIC | GrossRevenue | Quantity × UnitPrice (Prior to active discount modifications) |
# MAGIC | Revenue | Quantity × UnitPrice × (1 − Discount Percentage) |
# MAGIC | COGS | Quantity × UnitCost tracking parameters |
# MAGIC | GrossProfit | Derived Net Revenue − Calculated COGS metrics |
# MAGIC | GrossMarginPct | (GrossProfit / Revenue) × 100 boundaries |

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE workspace.silver.fact_sales AS
# MAGIC SELECT
# MAGIC     -- Degenerate Dimensions (Transaction transactional identifiers)
# MAGIC     d.OrderID,
# MAGIC     d.LineNo,
# MAGIC
# MAGIC     -- Foreign keys pointing to conformed dimensions
# MAGIC     cust.CustomerKey,
# MAGIC     cust.GeographyKey,
# MAGIC     prod.ProductKey,
# MAGIC     ship.ShipperKey,
# MAGIC     CAST(DATE_FORMAT(o.OrderDate, 'yyyyMMdd') AS INT)          AS DateKey,
# MAGIC
# MAGIC     -- Transactional Measures
# MAGIC     o.EmployeeID,
# MAGIC     d.Quantity,
# MAGIC     d.UnitPrice,
# MAGIC     d.Discount,
# MAGIC     o.Freight,
# MAGIC
# MAGIC     -- Financial Metric Formulations
# MAGIC     ROUND(d.Quantity * d.UnitPrice, 2)                         AS GrossRevenue,
# MAGIC     ROUND(d.Quantity * d.UnitPrice * (1 - d.Discount), 2)      AS Revenue,
# MAGIC     -- Bugfix: Adjusted p.UnitCost mapping replacing unreferenced p.TotalCost structural fields
# MAGIC     ROUND(d.Quantity * p.UnitCost, 2)                          AS COGS,
# MAGIC     ROUND(
# MAGIC         d.Quantity * d.UnitPrice * (1 - d.Discount)
# MAGIC         - d.Quantity * p.UnitCost, 2
# MAGIC     )                                                          AS GrossProfit,
# MAGIC     -- Derived KPI measure persisted directly inside the core fact layer to accelerate dashboard analytical steps
# MAGIC     ROUND(
# MAGIC         (d.Quantity * d.UnitPrice * (1 - d.Discount)
# MAGIC         - d.Quantity * p.UnitCost)
# MAGIC         / NULLIF(d.Quantity * d.UnitPrice * (1 - d.Discount), 0) * 100, 2
# MAGIC     )                                                          AS GrossMarginPct
# MAGIC
# MAGIC FROM workspace.silver.clean_order_details d
# MAGIC INNER JOIN workspace.silver.clean_orders o
# MAGIC     ON d.OrderID = o.OrderID
# MAGIC LEFT JOIN workspace.silver.clean_products p
# MAGIC     ON d.ProductID = p.ProductID
# MAGIC LEFT JOIN workspace.silver.dim_customers cust
# MAGIC     ON o.CustomerID = cust.CustomerID
# MAGIC LEFT JOIN workspace.silver.dim_products prod
# MAGIC     ON d.ProductID = prod.ProductID
# MAGIC LEFT JOIN workspace.silver.dim_shippers ship
# MAGIC     ON o.ShipperID = ship.ShipperID;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛡️ 7. Automated Data Quality Gate (Python Engine)
# MAGIC
# MAGIC Utilizing Python scripting to instantiate programmatic control gates beyond SQL boundaries:
# MAGIC The `raise Exception()` method breaks orchestration runs instantly if critical quality targets miss thresholds.
# MAGIC The final stage script (05_gold_kpis) will be barred from execution if anomalies trigger a fault.

# COMMAND ----------

dq_results = spark.sql("""
    SELECT
        SUM(CASE WHEN CustomerKey  IS NULL THEN 1 ELSE 0 END) AS null_cust,
        SUM(CASE WHEN ProductKey   IS NULL THEN 1 ELSE 0 END) AS null_prod,
        SUM(CASE WHEN DateKey      IS NULL THEN 1 ELSE 0 END) AS null_date,
        SUM(CASE WHEN GeographyKey IS NULL THEN 1 ELSE 0 END) AS null_geo,
        SUM(CASE WHEN COGS         IS NULL THEN 1 ELSE 0 END) AS null_cogs,
        SUM(CASE WHEN GrossProfit  IS NULL THEN 1 ELSE 0 END) AS null_gp,
        SUM(CASE WHEN Revenue < 0  THEN 1 ELSE 0 END)         AS neg_revenue,
        ROUND(SUM(Revenue), 2)                                AS total_revenue,
        ROUND(SUM(GrossProfit), 2)                            AS total_gp,
        COUNT(*)                                              AS total_rows
    FROM workspace.silver.fact_sales
""").collect()[0]

EXPECTED_REVENUE = 13321238.00
THRESHOLD = 50.00

print("=" * 55)
print("     AUTOMATED DATA QUALITY GATE — fact_sales")
print("=" * 55)

checks = {
    "NULL CustomerKey":  dq_results["null_cust"],
    "NULL ProductKey":   dq_results["null_prod"],
    "NULL DateKey":      dq_results["null_date"],
    "NULL GeographyKey": dq_results["null_geo"],
    "NULL COGS":         dq_results["null_cogs"],
    "NULL GrossProfit":  dq_results["null_gp"],
    "Negative Revenue":  dq_results["neg_revenue"],
}

all_ok = True
for check, count in checks.items():
    count = count or 0
    status = "✅ OK" if count == 0 else f"❌ FAIL — {count:,} records flagged!"
    if count > 0:
        all_ok = False
    print(f"  {check:<22} {status}")

# Financial Ledger Reconciliation Verification
actual_rev = dq_results["total_revenue"] or 0
actual_gp = dq_results["total_gp"] or 0
rev_diff = abs(actual_rev - EXPECTED_REVENUE)
margin = round(actual_gp / actual_rev * 100, 1) if actual_rev > 0 else 0

print(
    f"\n  Total rows:    {dq_results['total_rows']:>12,}  (Expected: 17,032)")
print(f"  Total Revenue: ${actual_rev:>14,.2f}  (Expected: $13,321,238)")
print(f"  Total GP:      ${actual_gp:>14,.2f}")
print(f"  Margin:        {margin:>13.1f}%  (Expected: ~16.2%)")

if rev_diff > THRESHOLD:
    all_ok = False
    print(f"\n  ❌ FAIL — Revenue baseline deviates by ${rev_diff:,.2f}!")

print("\n" + "=" * 55)
if not all_ok:
    raise Exception(
        "DQ Gate FAIL — Resolve underlying metrics before releasing Gold summaries!")

print(" STATUS: 🟢 fact_sales verified — Proceeding with 05_gold_kpis")
print("=" * 55)
