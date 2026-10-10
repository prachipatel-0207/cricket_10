# Banking CSV capstone: Airflow, Snowflake, dbt

Start with [the project file map](PROJECT_FILE_MAP.md) to understand each file,
then use [tomorrow's start checklist](TOMORROW_START_CHECKLIST.md) when working
with a new CSV.

This project processes the 45-column banking CSV from
`C:\Users\kenexai\Desktop\capstone\incoming_data`. The source file used for
validation is `banking_datset_1.csv` (1,693,158 rows). The input folder is
separate from the Astro project and is bind-mounted into Airflow at
`/usr/local/airflow/include/data`.

## What happens

1. **Airflow** runs `ingest_and_transform_pipeline` when you trigger it.
2. **Bronze** stores every CSV field as text in
   `CAPSTONE_DB.BRONZE.RAW_BANKING_TRANSACTIONS`. `FILE_AUDIT` tracks the
   filename, result, and loaded row count. A successfully loaded filename is
   skipped on later runs. The loader uploads large files in 50,000-row
   batches and restarts an incomplete filename from the beginning.
3. **dbt Silver** parses dates, numbers, and fraud flags. It creates one
   transaction per ID plus customer, account, branch, card, loan, and
   complaint views. Source dates use `YYYY-MM-DD`, `MM/DD/YYYY`, and
   `DD-MM-YYYY` formats.
4. **dbt Gold** creates daily transaction, channel, fraud, customer segment,
   loan, and complaint summaries. Monetary transaction summaries are kept
   separate by currency; no exchange-rate conversion is performed.

Silver and Gold are Snowflake **views**: saved SQL queries over Bronze, not
extra physical copies of the full CSV.

## Start and run on this laptop

1. Start Docker Desktop and confirm `docker info` works.
2. In this project directory, keep your Snowflake details in `.env`. If you
   change them, run `python setup_local.py` to regenerate the private
   `airflow_settings.yaml`.
3. Run `astro dev start -i capstone-local/airflow:validated -n`. The image
   tag is the local runtime that passed this project's Airflow tests. Astro
   prints the current Airflow UI URL; the port may change after a restart.
4. Put a CSV with the **same 45 headers in the same order** in
   `C:\Users\kenexai\Desktop\capstone\incoming_data`. Use a simple, new
   filename such as `banking_2026_10_10.csv` for each new batch.
5. In Airflow, trigger `ingest_and_transform_pipeline`. It is manual by
   default. Wait for `load_bronze` and all dbt tasks to show success.
6. Check Snowflake:

   ```sql
   SELECT FILENAME, ROWSLOADED, STATUS
   FROM CAPSTONE_DB.BRONZE.FILE_AUDIT
   ORDER BY LOAD_TIMESTAMP DESC;

   SELECT COUNT(*) FROM CAPSTONE_DB.BRONZE.RAW_BANKING_TRANSACTIONS;
   SELECT COUNT(*) FROM CAPSTONE_DB.SILVER.FACT_TRANSACTIONS;
   SELECT * FROM CAPSTONE_DB.GOLD.DAILY_TRANSACTION_KPIS LIMIT 10;
   SELECT * FROM CAPSTONE_DB.GOLD.CHANNEL_PERFORMANCE LIMIT 10;
   ```

For a manual check, compare `ROWSLOADED` in `BRONZE.FILE_AUDIT` with the
number of data rows in the CSV. Check the Airflow dbt tasks and tests are
green, then inspect a few Silver rows and Gold summary totals in Snowflake.

To stop only this project's local Airflow services, run `astro dev stop`.

## When the CSV structure changes

Change the Bronze table in `snowflake_init.sql`, the `CSV_COLUMNS` mapping in
`dags/pipeline_template.py`, and the Silver/Gold dbt models. A header-only
rename or a changed column order is enough to require a mapping update. The
existing `CREATE TABLE IF NOT EXISTS` statement will not alter a table that
already exists. Keep `.env` and customer data private.

The original five-column example table and views were replaced for this
dataset. The old empty `BRONZE.RAW_DATA` table may still exist in Snowflake;
the current pipeline does not use it.
