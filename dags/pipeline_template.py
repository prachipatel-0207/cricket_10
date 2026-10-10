"""Load the banking CSV into Bronze, then run the Silver and Gold dbt models."""

from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import csv
import re

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig

INPUT_FOLDER = Path("/usr/local/airflow/include/data")
DBT_PROJECT_PATH = "/usr/local/airflow/dbt_project"
CHUNK_ROWS = 50_000
CSV_COLUMNS = (
    "TRANSACTION_ID", "TRANSACTION_DATE", "TRANSACTION_TYPE", "AMOUNT",
    "CURRENCY", "TRANSACTION_STATUS", "PAYMENT_CHANNEL", "PAYMENT_METHOD",
    "RUNNING_BALANCE", "IS_FLAGGED_FRAUD", "MERCHANT_NAME", "MERCHANT_CATEGORY",
    "CUSTOMER_ID", "CUSTOMER_NAME", "CUSTOMER_DOB", "CUSTOMER_GENDER",
    "CUSTOMER_EMAIL", "CUSTOMER_PHONE", "CUSTOMER_CITY", "CUSTOMER_COUNTRY",
    "CUSTOMER_SEGMENT", "KYC_STATUS", "CUSTOMER_ANNUAL_INCOME", "CREDIT_SCORE",
    "ACCOUNT_ID", "ACCOUNT_TYPE", "ACCOUNT_OPEN_DATE", "ACCOUNT_STATUS",
    "BRANCH_ID", "BRANCH_NAME", "CREDIT_CARD_ID", "CARD_TYPE",
    "CARD_NETWORK", "CREDIT_LIMIT", "CARD_STATUS", "LOAN_ID", "LOAN_TYPE",
    "LOAN_AMOUNT", "INTEREST_RATE", "TENURE_MONTHS", "LOAN_STATUS",
    "COMPLAINT_ID", "COMPLAINT_CATEGORY", "COMPLAINT_PRIORITY", "COMPLAINT_STATUS",
)


def load_csv_files():
    files = sorted(INPUT_FOLDER.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"Put a CSV in {INPUT_FOLDER} before triggering this DAG")

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for file in files:
                if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv", file.name):
                    raise ValueError(f"Use a simple CSV filename: {file.name}")
                cursor.execute(
                    "SELECT COUNT(*) FROM BRONZE.FILE_AUDIT "
                    "WHERE FILENAME = %s AND STATUS = 'SUCCESS'",
                    (file.name,),
                )
                if cursor.fetchone()[0]:
                    continue

                name_literal = file.name.replace("'", "''")
                target_columns = ", ".join((*CSV_COLUMNS, "SOURCEFILE"))
                source_columns = ", ".join(
                    f"t.${position}" for position in range(1, len(CSV_COLUMNS) + 1)
                )
                try:
                    with file.open(encoding="utf-8-sig", newline="") as source:
                        reader = csv.reader(source)
                        header = next(reader)
                        if [name.lower() for name in CSV_COLUMNS] != header:
                            raise ValueError(f"Unexpected column names or order in {file.name}")

                        # A failed earlier run may have loaded some chunks. Clear
                        # only this filename, then reload every chunk exactly once.
                        cursor.execute(
                            "DELETE FROM BRONZE.RAW_BANKING_TRANSACTIONS "
                            "WHERE SOURCEFILE = %s",
                            (file.name,),
                        )
                        conn.commit()
                        expected_rows = 0
                        with TemporaryDirectory(prefix="capstone_csv_") as temp_dir:
                            part = 0
                            while True:
                                part += 1
                                chunk_name = f"{file.stem}_part{part:04d}.csv"
                                chunk_path = Path(temp_dir) / chunk_name
                                chunk_rows = 0
                                with chunk_path.open(
                                    "w", encoding="utf-8", newline=""
                                ) as output:
                                    writer = csv.writer(output)
                                    writer.writerow(header)
                                    for row in reader:
                                        if len(row) != len(CSV_COLUMNS):
                                            raise ValueError(
                                                f"Bad column count in {file.name}"
                                            )
                                        writer.writerow(row)
                                        chunk_rows += 1
                                        if chunk_rows == CHUNK_ROWS:
                                            break
                                if chunk_rows == 0:
                                    chunk_path.unlink()
                                    break

                                path_literal = chunk_path.as_posix().replace("'", "''")
                                cursor.execute(
                                    f"PUT 'file://{path_literal}' @BRONZE.MY_STAGE "
                                    "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                                )
                                cursor.execute(
                                    f"COPY INTO BRONZE.RAW_BANKING_TRANSACTIONS "
                                    f"({target_columns}) "
                                    f"FROM (SELECT {source_columns}, '{name_literal}' "
                                    f"FROM @BRONZE.MY_STAGE/{chunk_name}.gz t) "
                                    "FILE_FORMAT = (FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT') "
                                    "ON_ERROR = ABORT_STATEMENT FORCE = TRUE"
                                )
                                cursor.execute(
                                    f"REMOVE @BRONZE.MY_STAGE/{chunk_name}.gz"
                                )
                                chunk_path.unlink()
                                expected_rows += chunk_rows
                                print(f"Loaded {expected_rows} rows from {file.name}")

                    cursor.execute(
                        "SELECT COUNT(*) FROM BRONZE.RAW_BANKING_TRANSACTIONS "
                        "WHERE SOURCEFILE = %s",
                        (file.name,),
                    )
                    count = cursor.fetchone()[0]
                    if count != expected_rows:
                        raise ValueError(
                            f"Loaded {count} rows, expected {expected_rows} from {file.name}"
                        )
                    cursor.execute(
                        "INSERT INTO BRONZE.FILE_AUDIT "
                        "(FILENAME, ROWSLOADED, STATUS) VALUES (%s, %s, 'SUCCESS')",
                        (file.name, count),
                    )
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    cursor.execute(
                        "INSERT INTO BRONZE.FILE_AUDIT "
                        "(FILENAME, ROWSLOADED, STATUS) VALUES (%s, 0, %s)",
                        (file.name, f"FAILED: {str(exc)[:450]}"),
                    )
                    conn.commit()
                    raise


with DAG(
    dag_id="ingest_and_transform_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["snowflake", "dbt", "medallion"],
) as dag:
    load_bronze = PythonOperator(task_id="load_bronze", python_callable=load_csv_files)
    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(dbt_project_path=DBT_PROJECT_PATH),
        profile_config=ProfileConfig(
            profile_name="capstone_dbt",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),
    )
    load_bronze >> dbt_transform
