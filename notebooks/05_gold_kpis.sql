-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 🥇 Gold Layer — Business KPIs (Pure SQL Engine)
-- MAGIC **Input:** `workspace.silver.fact_sales`, `workspace.silver.dim_*`
-- MAGIC **Output:** `workspace.gold.kpi_*`
-- MAGIC
-- MAGIC ## Materialized KPI Views Resolving Core Requirements:
-- MAGIC | Index | Target Structure | Corporate Tracking Intent |
-- MAGIC |---|--------|--------|
-- MAGIC | 1 | kpi_geo_performance | YoY analytical distributions parsed across Region, Country, and Division layers |
-- MAGIC | 2 | kpi_product_performance | Product Line efficiency indicators, inventory segment tracking, and margin profiling |
-- MAGIC | 3 | kpi_customer_metrics | Aggregated average transaction metrics and lifetime value calculations evaluated per client record |
-- MAGIC | 4 | kpi_top_customers | Dedicated strategic account profiling — isolating the Top 10 corporate drivers yearly |
-- MAGIC | 5 | kpi_executive_snapshot | Strategic overview checking continuous user-base expansion patterns (YTD vs LYTD frameworks) |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI 1 — Geographical and Chronological Matrix Tracking (YoY & Margins)
-- MAGIC **Analytical Target:** Analyze sales figures, compare YoY, by Region, Country, Division.
-- MAGIC
-- MAGIC Primary source data mapping foundational blocks for the strategic Power BI corporate canvas dashboard layout.
-- MAGIC Year-over-Year percentage adjustments calculated dynamically through LAG window analytic functions.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_geo_performance AS
WITH yearly_base AS (
    SELECT
        d.Year,
        d.Quarter,
        d.QuarterName,
        d.Month,
        d.MonthName,
        d.MonthShort,
        g.Region,
        g.Country,
        ROUND(SUM(f.Revenue), 2)                                    AS TotalRevenue,
        ROUND(SUM(f.COGS), 2)                                       AS TotalCOGS,
        ROUND(SUM(f.GrossProfit), 2)                                AS TotalGrossProfit,
        ROUND(SUM(f.GrossProfit) / SUM(f.Revenue) * 100, 2)        AS GrossMarginPct,
        COUNT(DISTINCT f.OrderID)                                   AS TotalOrders,
        COUNT(DISTINCT f.CustomerKey)                               AS UniqueCustomers
    FROM workspace.silver.fact_sales f
    INNER JOIN workspace.silver.dim_date d      ON f.DateKey      = d.DateKey
    INNER JOIN workspace.silver.dim_geography g ON f.GeographyKey = g.GeographyKey
    GROUP BY
        d.Year, d.Quarter, d.QuarterName,
        d.Month, d.MonthName, d.MonthShort,
        g.Region, g.Country
)
SELECT
    *,
    -- Dynamic YoY Revenue calculation benchmarking matching spatial parameters (Region + Country) across timeline horizons
    ROUND(
        (TotalRevenue - LAG(TotalRevenue) OVER (
            PARTITION BY Region, Country, Month
            ORDER BY Year
        )) /
        NULLIF(LAG(TotalRevenue) OVER (
            PARTITION BY Region, Country, Month
            ORDER BY Year
        ), 0) * 100, 1
    ) AS YoY_RevenuePct
FROM yearly_base
ORDER BY Year, Month, Region, Country;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI 2 — Product Performance Architecture (Product Group & Margins)
-- MAGIC **Analytical Target:** Analyze by Product Line and Product Group, measure and track margins.
-- MAGIC
-- MAGIC CategoryName parameters sourced from dim_products evaluate directly against target ProductGroups (e.g., Women's Clothes, Men's Footwear).
-- MAGIC GrossProfit and ActualMarginPct outputs scale efficiently since underlying UnitCost data structures were normalized in script 04.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_product_performance AS
SELECT
    d.Year,
    d.Quarter,
    d.QuarterName,
    p.ProductGroup,
    p.ProductName,
    p.BaseMarginPct                                             AS ListMarginPct,
    SUM(f.Quantity)                                            AS TotalQuantity,
    ROUND(SUM(f.Revenue), 2)                                   AS TotalRevenue,
    ROUND(SUM(f.COGS), 2)                                      AS TotalCOGS,
    ROUND(SUM(f.GrossProfit), 2)                               AS TotalGrossProfit,
    -- Effective transactional margin realized following discounting deductions
    ROUND(SUM(f.GrossProfit) / SUM(f.Revenue) * 100, 2)       AS ActualMarginPct,
    -- Evaluates margin contraction margins representing the direct financial impact of user pricing discounts
    ROUND(p.BaseMarginPct -
          SUM(f.GrossProfit) / SUM(f.Revenue) * 100, 2)       AS DiscountImpactPct
FROM workspace.silver.fact_sales f
INNER JOIN workspace.silver.dim_date     d ON f.DateKey    = d.DateKey
INNER JOIN workspace.silver.dim_products p ON f.ProductKey = p.ProductKey
GROUP BY
    d.Year, d.Quarter, d.QuarterName,
    p.ProductGroup, p.ProductName, p.BaseMarginPct
ORDER BY d.Year, p.ProductGroup, TotalRevenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI 3 — Customer Engagement Metric Architecture & Average Value Synthetics
-- MAGIC **Analytical Target:** Compute average sales per transaction and per customer, analyze by Customer.
-- MAGIC
-- MAGIC Establishes three layers of calculation detailing purchasing patterns:
-- MAGIC - AvgSalesPerTransaction = Total revenue size evaluated over entire unique orders (Average Order Value - AOV)
-- MAGIC - AvgSalesPerLine = Revenue return mapped to isolated single item line transactions
-- MAGIC - AvgRevenuePerCustomer = Unified revenue pools evaluated across active client lists

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_customer_metrics AS
SELECT
    d.Year,
    g.Region,
    g.Country,
    c.CustomerID,
    c.CustomerName,
    ROUND(SUM(f.Revenue), 2)                                        AS CustomerTotalRevenue,
    ROUND(SUM(f.GrossProfit), 2)                                    AS CustomerTotalGrossProfit,
    ROUND(SUM(f.GrossProfit) / SUM(f.Revenue) * 100, 2)            AS CustomerMarginPct,
    COUNT(DISTINCT f.OrderID)                                       AS TotalOrders,
    SUM(f.Quantity)                                                 AS TotalItemsBought,
    -- Standard calculation mapping structural Average Order Value (AOV) metrics
    ROUND(SUM(f.Revenue) / COUNT(DISTINCT f.OrderID), 2)           AS AvgSalesPerTransaction,
    -- Tracks processing return evaluated over separate database transactional records
    ROUND(AVG(f.Revenue), 2)                                       AS AvgSalesPerLine,
    -- Analytical rank indexing client values inside single fiscal boundaries
    RANK() OVER (
        PARTITION BY d.Year
        ORDER BY SUM(f.Revenue) DESC
    )                                                               AS RevenueRank
FROM workspace.silver.fact_sales f
INNER JOIN workspace.silver.dim_date      d ON f.DateKey      = d.DateKey
INNER JOIN workspace.silver.dim_customers c ON f.CustomerKey  = c.CustomerKey
INNER JOIN workspace.silver.dim_geography g ON f.GeographyKey = g.GeographyKey
GROUP BY
    d.Year, g.Region, g.Country,
    c.CustomerID, c.CustomerName
ORDER BY d.Year, CustomerTotalRevenue DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI 4 — Strategic Client Profiling (Top 10 Accounts Annually)
-- MAGIC **Analytical Target:** Analyze sales data by Customer.
-- MAGIC
-- MAGIC Leverages analytical RANK() window structures partitioned across individual yearly constraints.
-- MAGIC Provides immediate transactional insights structured for specialized account management dashboards inside BI environments.

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_top_customers AS
SELECT *
FROM (
    SELECT
        d.Year,
        c.CustomerID,
        c.CustomerName,
        g.Region,
        g.Country,
        ROUND(SUM(f.Revenue), 2)                                AS TotalRevenue,
        ROUND(SUM(f.GrossProfit), 2)                            AS TotalGrossProfit,
        ROUND(SUM(f.GrossProfit) / SUM(f.Revenue) * 100, 2)    AS GrossMarginPct,
        COUNT(DISTINCT f.OrderID)                               AS TotalOrders,
        RANK() OVER (
            PARTITION BY d.Year
            ORDER BY SUM(f.Revenue) DESC
        )                                                       AS RevenueRank
    FROM workspace.silver.fact_sales f
    INNER JOIN workspace.silver.dim_date      d ON f.DateKey      = d.DateKey
    INNER JOIN workspace.silver.dim_customers c ON f.CustomerKey  = c.CustomerKey
    INNER JOIN workspace.silver.dim_geography g ON f.GeographyKey = g.GeographyKey
    GROUP BY
        d.Year, c.CustomerID, c.CustomerName,
        g.Region, g.Country
)
WHERE RevenueRank <= 10
ORDER BY Year, RevenueRank;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## KPI 5 — Executive Performance Summary (YTD vs LYTD Frameworks)
-- MAGIC **Analytical Target:** Follow up on customer growth over time (YTD vs Last Year-To-Date).
-- MAGIC
-- MAGIC Executed via three sequential Common Table Expression (CTE) processing segments:
-- MAGIC 1. MonthlyBase — Compiles monthly net revenue baselines and profiles active purchasing users
-- MAGIC 2. YTD_Calculation — Instantiates running aggregate calculations utilizing specialized SUM OVER operations
-- MAGIC 3. Final Selection — Maps structural LYTD metrics referencing past time intervals (12-month window step lookback)

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold.kpi_executive_snapshot AS
WITH MonthlyBase AS (
    -- Step 1: Compute monthly performance foundations and active account distributions
    SELECT
        d.Year,
        d.Month,
        d.MonthName,
        d.MonthShort,
        ROUND(SUM(f.Revenue), 2)            AS MonthlyRevenue,
        ROUND(SUM(f.GrossProfit), 2)        AS MonthlyGrossProfit,
        COUNT(DISTINCT f.OrderID)           AS MonthlyOrders,
        COUNT(DISTINCT f.CustomerKey)       AS MonthlyActiveCustomers
    FROM workspace.silver.fact_sales f
    INNER JOIN workspace.silver.dim_date d ON f.DateKey = d.DateKey
    GROUP BY d.Year, d.Month, d.MonthName, d.MonthShort
),
YTD_Calculation AS (
    -- Step 2: Accumulate continuous tracking indices restricted within explicit calendar years
    SELECT
        Year,
        Month,
        MonthName,
        MonthShort,
        MonthlyRevenue,
        MonthlyGrossProfit,
        MonthlyOrders,
        MonthlyActiveCustomers,
        ROUND(SUM(MonthlyRevenue) OVER (
            PARTITION BY Year ORDER BY Month
        ), 2)                               AS Revenue_YTD,
        ROUND(SUM(MonthlyGrossProfit) OVER (
            PARTITION BY Year ORDER BY Month
        ), 2)                               AS GrossProfit_YTD,
        SUM(MonthlyOrders) OVER (
            PARTITION BY Year ORDER BY Month
        )                                   AS Orders_YTD,
        SUM(MonthlyActiveCustomers) OVER (
            PARTITION BY Year ORDER BY Month
        )                                   AS Customers_YTD
    FROM MonthlyBase
)
-- Step 3: Establish comparative lines (LYTD) via a historical 12-month analytical LAG operation
SELECT
    Year,
    Month,
    MonthName,
    MonthShort,
    MonthlyRevenue,
    Revenue_YTD,
    -- LYTD maps directly to the parallel YTD month registered in the preceding calendar cycle
    LAG(Revenue_YTD, 12) OVER (
        ORDER BY Year, Month
    )                                       AS Revenue_LYTD,
    -- Volume growth size tracker expressed as raw dollar differences
    ROUND(Revenue_YTD - LAG(Revenue_YTD, 12) OVER (
        ORDER BY Year, Month
    ), 2)                                   AS YoY_Growth_Amt,
    -- Growth rate tracking index expressed as percentages
    ROUND(
        (Revenue_YTD - LAG(Revenue_YTD, 12) OVER (ORDER BY Year, Month)) /
        NULLIF(LAG(Revenue_YTD, 12) OVER (ORDER BY Year, Month), 0) * 100, 2
    )                                       AS YoY_Growth_Pct,
    GrossProfit_YTD,
    ROUND(GrossProfit_YTD / NULLIF(Revenue_YTD, 0) * 100, 2) AS MarginPct_YTD,
    Orders_YTD,
    Customers_YTD
FROM YTD_Calculation
ORDER BY Year, Month;

-- COMMAND ----------

-- Quick validation verification run — Expected baseline Revenue output for 2010 boundary: ~4.89M
SELECT Year, Month, Revenue_YTD, Revenue_LYTD, YoY_Growth_Pct
FROM workspace.gold.kpi_executive_snapshot
ORDER BY Year, Month
LIMIT 20;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ✅ Gold Summary
-- MAGIC
-- MAGIC | Target View | Business Tracking Request |
-- MAGIC |--------|-------------------|
-- MAGIC | kpi_geo_performance | YoY Revenue analysis, Region, Country, Division spatial parameters, and Profitability Margins |
-- MAGIC | kpi_product_performance | Catalog adjustments, Pricing configurations, Actual Margin returns, and Discount Impacts |
-- MAGIC | kpi_customer_metrics | Average transaction sizing analytics and multi-tier transaction value insights |
-- MAGIC | kpi_top_customers | Focus evaluation pinpointing Top 10 customer accounts driving primary revenue generation |
-- MAGIC | kpi_executive_snapshot | Continuous operational visibility comparing YTD vs LYTD expansion trends |
-- MAGIC
-- MAGIC All analytics targets are stored inside `workspace.gold.*` and can be accessed directly through Databricks Partner Connect configurations to instantly map into native Power BI environments.