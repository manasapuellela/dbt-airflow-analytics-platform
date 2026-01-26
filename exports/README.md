# Exports

This folder will contain CSV extracts ready for BI tools. The Airflow task `generate_bi_exports`
creates `daily_metrics.csv` from `marts.fact_trips`.

Load into BI tools:
- **QuickSight**: create a dataset from a CSV file in S3 (or local upload).
- **Tableau**: connect to the CSV and build a dashboard.
- **Power BI**: use Get Data -> Text/CSV.
