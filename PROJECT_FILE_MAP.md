# Project file map

This is the current **banking CSV** project. Read the files in this order:
`dags/pipeline_template.py` -> `snowflake_init.sql` ->
`dbt_project/models/silver/` -> `dbt_project/models/gold/`.

## How one run works

1. You put a `.csv` file in `C:\Users\kenexai\Desktop\capstone\incoming_data`.
2. You manually trigger `ingest_and_transform_pipeline` in Airflow.
3. The loader skips filenames already marked `SUCCESS` in `BRONZE.FILE_AUDIT`.
   It loads other files into Snowflake Bronze in batches of 50,000 rows.
4. Airflow runs dbt. Silver cleans the raw values; Gold makes summaries.
   Airflow also runs the dbt tests defined in `models/schema.yml`.

Putting a file in the folder **does not automatically trigger** the DAG.
Silver and Gold are Snowflake views, so they query the current Bronze data.

## Files in the project root

| File | What it does | Change for a new CSV schema? |
| --- | --- | --- |
| `README.md` | Current setup and run instructions. | Update examples if the dataset changes. |
| `STEP_BY_STEP_MASTER_GUIDE.md` | Beginner explanation of the banking pipeline. | Update the story if the dataset changes. |
| `Dockerfile` | Defines the Airflow runtime image. | Usually no. |
| `requirements.txt` | Python packages for Snowflake, dbt and Cosmos. | Only for new package requirements. |
| `packages.txt` | Optional operating-system packages for the image; currently only a placeholder. | Usually no. |
| `docker-compose.override.yml` | Mounts the dbt project and the separate `incoming_data` folder into Airflow containers. | Only if you move the input folder. |
| `snowflake_init.sql` | Creates the warehouse, database, schemas, CSV file format, stage, audit table and banking Bronze table. | **Yes:** create a Bronze table with the new columns, then execute that SQL in Snowflake. Editing the file alone changes nothing in Snowflake. |
| `setup_local.py` | Reads `.env` and generates the Airflow Snowflake connection file. | Only if connection details change. |
| `.env.example` | Blank example of required Snowflake settings. | Usually no. |
| `.env` | Your real Snowflake settings. Private; do not share it. | Only if your account, user, password, role, database or warehouse changes. |
| `airflow_settings.yaml` | Generated private Airflow connection settings. | Run `python setup_local.py` after changing `.env`; do not hand-edit it. |
| `.dockerignore`, `.gitignore` | Keep secrets, generated files and CSVs out of the image or Git. | Usually no. |
| `.astro/` | Local Astro project state. | Do not edit for a CSV change. |

The `include/`, `plugins/` and `tests/` directories are Astro project
scaffolding. This pipeline's CSV input is the separate `incoming_data` folder.

## Airflow DAG files

| File | What it does | Change for a new CSV schema? |
| --- | --- | --- |
| `dags/pipeline_template.py` | Main DAG: scans `*.csv`, checks the audit table, checks header/order, uploads chunks, copies rows to Bronze and launches dbt. `schedule=None` means manual runs. | **Yes:** update `CSV_COLUMNS` and every reference to the target Bronze table. Keep the audit/retry logic. |
| `dags/test_snowflake.py` | Separate optional DAG that runs a small Snowflake connection query. | Usually no. It is not the data pipeline. |

## dbt configuration

| File | What it does | Change for a new CSV schema? |
| --- | --- | --- |
| `dbt_project/profiles.yml` | Uses Snowflake settings from environment variables. | Usually no. |
| `dbt_project/dbt_project.yml` | dbt project settings; Silver and Gold models are views in those schemas. | Usually no. |
| `dbt_project/macros/generate_schema_name.sql` | Places models in the exact `SILVER` or `GOLD` schema rather than prefixing the schema name. | Usually no. |
| `dbt_project/models/sources.yml` | Tells dbt which Bronze table is the source. | **Yes** if the new dataset uses a new Bronze table. |
| `dbt_project/models/schema.yml` | Tests important IDs for non-null and unique values. | **Yes** if the new dataset has different IDs or model names. |

## Silver models: cleaned detail

| File | What it produces |
| --- | --- |
| `silver/stg_banking_rows.sql` | Parses raw text into dates, numbers, fraud flags and cleaned strings. |
| `silver/fact_transactions.sql` | One row per transaction ID. |
| `silver/dim_customer.sql` | One row per customer ID. |
| `silver/dim_account.sql` | One row per account ID. |
| `silver/dim_branch.sql` | One row per branch ID. |
| `silver/dim_card.sql` | One row per credit card ID. |
| `silver/dim_loan.sql` | One row per loan ID. |
| `silver/dim_complaint.sql` | One row per complaint ID. |

All these files are in `dbt_project/models/silver/`. For a different dataset,
update or remove **every** model that refers to banking-only columns. A dbt
`ref('model_name')` means one model depends on another.

## Gold models: report summaries

| File | What it produces |
| --- | --- |
| `gold/daily_transaction_kpis.sql` | Daily counts, successful amounts and fraud counts by currency. |
| `gold/channel_performance.sql` | Counts, success rates and amounts by payment channel and currency. |
| `gold/fraud_monitoring.sql` | Fraud counts, amounts and rates by day and currency. |
| `gold/customer_segment_summary.sql` | Customer-segment transaction and fraud summary by currency. |
| `gold/loan_portfolio.sql` | Loan counts and average rates/tenures by loan type and status. |
| `gold/complaint_summary.sql` | Complaint counts by category, priority and status. |

These files are in `dbt_project/models/gold/`. Replace banking reports with
the reports required by the new assignment. The current transaction reports
keep currencies separate because the CSV has no exchange rates.

## Data folder

`C:\Users\kenexai\Desktop\capstone\incoming_data` is the input folder. It
currently holds `banking_datset_1.csv`; it is **not** inside the Astro project.
Use a new filename for each new batch. The audit table uses the filename to
decide whether a file has already succeeded.
