# Tomorrow: start and adapt this project without AI

Use PowerShell. The project is at
`C:\Users\kenexai\Desktop\capstone\capstone` and CSVs go in
`C:\Users\kenexai\Desktop\capstone\incoming_data`.

## 1. Start Airflow

1. Open Docker Desktop and wait until its engine is running.
2. Open PowerShell and run:

   ```powershell
   cd C:\Users\kenexai\Desktop\capstone\capstone
   astro dev ps
   ```

3. If the project is stopped, run:

   ```powershell
   astro dev start -i capstone-local/airflow:validated -n
   ```

4. Open the Airflow URL printed by Astro. It was `http://localhost:16016`
   when tested, but the port can change. `astro dev ps` shows the current port.

The Snowflake credentials are already in the private `.env` file and the
Airflow connection was already generated. If you change `.env`, run
`python setup_local.py` before starting Airflow again. Never show your
password in a screenshot or submission.

## 2. Inspect the new CSV **before** running the DAG

Put a copy of the file in `incoming_data` and inspect its first line:

```powershell
Get-Content C:\Users\kenexai\Desktop\capstone\incoming_data\YOUR_FILE.csv -TotalCount 1
```

Also note the number of columns, date formats, number formats, unique ID
column, and the summaries your assignment asks for.

### If all 45 headers and their order match `banking_datset_1.csv`

Use a **new filename**. You do not need to edit code or Snowflake tables.
Go straight to step 4. Changed row values are fine; changed column names or
column order are a schema change.

### If the headers or order are different

Do step 3 first. The existing banking loader checks for its exact header and
will reject a different CSV.

## 3. Adapt the schema and SQL when columns differ

1. In Snowflake, create a **new Bronze table** for the new dataset. Use one
   text/VARCHAR column per CSV field and keep `SOURCEFILE` and
   `LOAD_TIMESTAMP`. Use `snowflake_init.sql` as an example. Execute the new
   `CREATE TABLE` in Snowflake; saving SQL in a file is not enough. A new
   table keeps the existing banking data separate. The existing database,
   schemas, file format, stage and audit table are already set up.
2. In `dags/pipeline_template.py`, change `CSV_COLUMNS` to the new CSV
   columns in **exact file order**. The current header check expects the
   CSV header to be lowercase versions of those SQL column names. If the
   supplied headers contain spaces or other special characters, make a
   Snowflake-safe column mapping and adjust the header check; do not merely
   paste unsafe names into SQL.
3. In that same DAG file, change all three references to
   `BRONZE.RAW_BANKING_TRANSACTIONS`: the retry `DELETE`, the `COPY INTO`,
   and the post-load `SELECT COUNT(*)`. Keep `SOURCEFILE` in the `COPY`
   target. Do **not** change the `FILE_AUDIT` table or retry logic.
4. In `dbt_project/models/sources.yml`, point dbt to the new Bronze table.
5. In `dbt_project/models/silver/`, make a cleaned staging model with the
   correct date/number conversions. Build detail models using the real IDs
   in the new CSV. Update or remove old banking models that no longer fit.
6. In `dbt_project/models/gold/`, write the summaries required by your
   assignment. Update or remove old banking reports that no longer fit.
7. In `dbt_project/models/schema.yml`, keep tests for IDs that should be
   present and unique; remove tests for deleted models or columns.

Before triggering, check the DAG appears in Airflow without an import error.
If dbt fails with an unknown column or missing model, check all SQL files
and their `ref(...)` links; Airflow tries to run every dbt model still in the
project.

## 4. Trigger the pipeline

In Airflow, open `ingest_and_transform_pipeline` and click **Trigger DAG**.
Wait for `load_bronze`, the dbt models and the dbt tests to become green.

**File arrival does not trigger the DAG.** The DAG has `schedule=None`, so
you must trigger it yourself. At the start of each run it scans every `.csv`
in `incoming_data`, skips filenames already marked `SUCCESS` in
`BRONZE.FILE_AUDIT`, and processes new or previously failed filenames.
Do not reuse an already successful filename for changed contents. Renaming
the same data to a new filename will load those records again.

## 5. Check the result manually

In Snowflake, first check the audit record:

```sql
SELECT FILENAME, ROWSLOADED, STATUS
FROM CAPSTONE_DB.BRONZE.FILE_AUDIT
ORDER BY LOAD_TIMESTAMP DESC;
```

For the current banking table, count the rows from one file:

```sql
SELECT COUNT(*)
FROM CAPSTONE_DB.BRONZE.RAW_BANKING_TRANSACTIONS
WHERE SOURCEFILE = 'YOUR_FILE.csv';
```

Use your **new Bronze table name** if you changed schemas. Compare this
count and `ROWSLOADED` with the CSV's data-row count (do not include its
header). Open a few Silver records and Gold summary rows in Snowflake and
check that they match the assignment. The separate validation script was
removed; it is not required.

## Quick fixes

| Problem | Check |
| --- | --- |
| DAG is not visible | Docker/Astro running? Python syntax or DAG import error? |
| `load_bronze` says unexpected columns | Header spelling, case and order versus `CSV_COLUMNS`. |
| New file is skipped | Its filename already has a `SUCCESS` audit record. |
| `COPY INTO` fails | Bronze table columns and file format; look at the task log. |
| dbt fails | Bronze source name, column names, `ref(...)` links and old models. |
| Row count is too low | Check the task log, audit row and CSV header/data row count. |

To stop only this project's Airflow containers after your work, run
`astro dev stop` from the project folder.
