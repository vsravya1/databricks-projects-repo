# 🏗️ Financial Data Transformation Using Medallion Architecture in Databricks

## 📌 Project Overview

This project demonstrates the implementation of the **Medallion Architecture** using **Databricks**, to transform raw financial data into actionable business intelligence. The pipeline progresses through **Bronze**, **Silver**, and **Gold** layers, ensuring clean, high-quality, and analytics-ready data for decision-making.

---

## 🧱 Medallion Architecture Layers

### 🟤 Bronze Layer - Raw Ingestion
- Source: Financial data ingested from **Amazon S3**
- Format: Raw JSON/CSV files
- Purpose: Preserve raw source-of-truth data with minimal processing

### ⚪ Silver Layer - Data Cleansing & Enrichment
- Applied extensive transformation logic to ensure data quality and consistency:
  - ✅ Standardized string fields (e.g., customer names, addresses)
  - ✅ Cleaned and cast numerical fields (e.g., income, credit scores, debt)
  - ✅ Parsed nested/complex fields like credit history into usable formats
  - ✅ Normalized loan types and other categorical variables
  - ✅ Deduplicated customer records using key fields and timestamps
  - ✅ Generated time-series compatible date fields for trend analysis

### 🟡 Gold Layer - Business Intelligence Aggregates
- Created business-facing, analytics-ready datasets:
  - 📊 **Customer Snapshot**: Latest profile for each customer
  - 📈 **Monthly Metrics**: Time-based metrics like delinquency rates
  - 👥 **Occupation Analytics**: Behavioral segmentation by occupation
  - 🎯 **High-Risk Customers**: Identified top 50 customers by debt risk

---

## 📊 Dashboard Insights

An interactive dashboard was built to present key findings:

| Metric                      | Value                    |
|----------------------------|--------------------------|
| Total Customers            | 12,500                   |
| Outstanding Debt           | $17.83M                  |
| High-Risk Customers        | 4,490                    |
| Average Customer LTV       | $3.51M                   |

**Visual Insights Include**:
- Payment behavior distributions
- Occupational financial trends
- Delinquency trends over time
- Risk profiling heatmaps

---

## 💡 Key Technologies Used

- **Databricks** (Spark-based data processing)
- **PySpark** (ETL transformations)
- **Delta Lake** (Reliable, ACID-compliant data lake storage)
- **Amazon S3** (Cloud storage for raw data)
- **SQL & Visualizations** (Dashboard creation within Databricks)

---

## 🎯 Key Takeaway

Implementing a **Medallion Architecture** ensures:
- Data quality at every transformation layer
- Scalable and maintainable pipelines
- Reliable analytics for real-time business decisions

This architecture is especially effective in **financial data** use cases where **accuracy**, **traceability**, and **performance** are critical.
