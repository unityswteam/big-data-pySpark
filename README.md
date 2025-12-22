Gold Reserves ETL Pipeline with Power BI Dashboard
📊 Project Overview
Business Problem: Central banks, financial institutions, and policymakers need accurate, up-to-date information on global gold reserves to make informed decisions about monetary policy, risk management, and investment strategies. However, gold reserve data comes in multiple formats (CSV, JSON, Parquet) from different sources, requiring a robust ETL pipeline to consolidate, clean, and analyze this information.

Dataset: Global gold reserves data from various countries and economic zones, including current and previous reserve values, reference dates, and units of measurement.


🏗️ Architecture Diagram
text
Data Sources → PySpark ETL → DuckDB → Power BI Dashboard
     ↓              ↓           ↓           ↓
   CSV         Transform   Analytics   Visualization
   JSON        Clean      SQL Queries  Reports
   Parquet     Integrate  Aggregation  Dashboards


Pipeline Flow:

Extract: Load data from CSV, JSON, and Parquet files using PySpark

Transform: Clean, standardize, and enrich data (country names, dates, calculations)

Integrate: Merge all sources into a unified dataset

Load: Store transformed data in DuckDB for analytics

Visualize: Connect Power BI to DuckDB for interactive dashboards

🚀 Installation & Setup
Prerequisites
Python 3.8+

Java 8+ (for PySpark)

Power BI Desktop

1. Install Dependencies
bash
# Create virtual environment (optional)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install required packages
pip install -r requirements.txt
2. Project Structure
text
project/
├── data/
│   ├── gold_reserves (1).csv
│   ├── gold_reserves (1).json
│   └── gold_reserves (1).parquet
├── etl_pipeline.py
├── requirements.txt
└── README.md
3. Run the ETL Pipeline
bash
# Ensure data files are in the 'data' folder
python etl_pipeline.py
4. Connect Power BI to DuckDB
Install DuckDB ODBC Driver:

Download from DuckDB ODBC Driver

Install following platform-specific instructions

Power BI Connection:

Open Power BI Desktop

Click "Get Data" → "ODBC"

Configure connection:

text
DSN: DuckDB
Database: warehouse/analytics.duckdb
Select "gold_reserves" table

Create Dashboard:

Build visualizations using:

Country-wise gold reserves (Bar chart)

Reserve trends over time (Line chart)

Top 10 countries by reserves (Card visual)

Reserve changes (UP/DOWN indicators)

👥 Team Contributions
Abebaw Addis : Spark Session and flow

Abinet Ayele: Extraction and  JSON transformation logic, Requirements gathering, architecture design, documentation
Daniel Hailu: Transforming  csv and Parquet
Yohannes K/maryam    1500066: PySpark ETL pipeline development, JSON transformation logic

Asregide Firdie: DuckDB integration, data modeling, orchestration setup,documentation

Daniel Hailu: Transforming  csv and Parquet, Power BI dashboard design, visualization, business metrics

🔄 Orchestration (Prefect Flow)
The ETL pipeline uses Prefect for workflow orchestration with the following key components:

Flow Structure:
get_spark(): Initialize Spark session

Extract tasks: extract_csv(), extract_json(), extract_parquet()

Transform tasks: Multiple JSON transformation strategies

integrate(): Merge all data sources

load_to_duckdb(): Load to analytics database

cleanup(): Resource management

Key Features:
Caching disabled for data freshness (cache_policy=NO_CACHE)

Multiple JSON handling strategies based on data structure

Automatic schema inference and type casting

Duplicate removal and data validation

Trend analysis (UP/DOWN/NO_CHANGE indicators)

Run Configuration:
python
# Customize file paths if needed
gold_reserves_etl(
    csv_path="data/gold_reserves (1).csv",
    parquet_path="data/gold_reserves (1).parquet",
    json_path="data/gold_reserves (1).json"
)
📈 Power BI Dashboard Components
Recommended Visualizations:
Global Gold Reserves Overview

Total global reserves metric

Country ranking table

Reserve distribution by continent

Trend Analysis

Monthly/quarterly changes

Top gainers/losers

Reserve growth rate

Comparative Analysis

Country comparison tool

Regional aggregates

Historical trends

Alert Dashboard

Significant changes (>10%)

New data arrivals

Data quality indicators

Refresh Schedule:
Set up scheduled refresh in Power BI Service

Configure pipeline to run daily/weekly

Monitor ETL job status and data quality

🔧 Troubleshooting
Common Issues:
Spark Java Error: Ensure Java 8+ is installed and JAVA_HOME is set

File Not Found: Check file paths in the data folder

Memory Issues: Reduce Spark partitions for large datasets

Power BI Connection: Verify ODBC driver installation and DSN configuration

Debug Mode:
python
# Enable debug logging
import logging
logging.basicConfig(level=logging.DEBUG)
📊 Expected Output
After running the pipeline:

✅ Transformed data in warehouse/analytics.duckdb

✅ Clean, standardized gold reserves dataset

✅ Ready for Power BI visualization

✅ Automated trend analysis and metrics

The pipeline processes ~100+ countries' gold reserve data, providing financial analysts with timely, accurate information for decision-making.


