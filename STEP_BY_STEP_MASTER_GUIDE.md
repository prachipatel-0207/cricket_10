# Beginner guide to this banking pipeline

The [README](README.md) has the commands and file locations.

Think of the CSV as a delivery:

- **Airflow** is the person following the checklist. It uploads the file and
  then tells dbt to run its SQL models.
- **Snowflake Bronze** is the receipt and original contents. It keeps the
  source columns as text, including optional blanks.
- **dbt Silver** is the cleaned record book. It turns dates and amounts into
  useful types, flags fraud as true/false, and keeps one row per transaction,
  customer, account, card, loan, or complaint ID.
- **dbt Gold** is the dashboard-ready summary: transaction counts and amounts,
  channel success rates, fraud counts, customer segments, loan counts, and
  complaint counts.

The file contains eight currencies. An amount in USD is not added to an amount
in INR or EUR in the Gold transaction summaries. The file does not specify a
loan currency, so the loan summary counts loans and reports average rates and
tenures without summing loan principal.

For the next file, use the same 45-column layout and a new filename in the
`incoming_data` folder, then trigger the Airflow DAG. Review `FILE_AUDIT`,
Bronze and Silver row counts, dbt test results, and a few Gold rows.
