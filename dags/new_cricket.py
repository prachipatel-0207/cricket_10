
from datetime import datetime
from pathlib import Path
import re
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import csv
import re
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from cosmos import DbtTaskGroup, ProfileConfig, ProjectConfig


INPUT_FOLDER = Path("/usr/local/airflow/include/data")
DBT_PROJECT_PATH = "/usr/local/airflow/dbt_project"

CSV_COLUMNS = (
    "DELIVERY_ID", "DELIVERY_TIMESTAMP", "INNINGS_NO", "OVER_NO",
    "BALL_IN_OVER", "RUNS_OFF_BAT", "EXTRA_TYPE", "EXTRA_RUNS",
    "TOTAL_RUNS", "IS_WICKET", "DISMISSAL_TYPE", "BALL_SPEED_KMPH",
    "NON_STRIKER_ID", "SHOT_TYPE", "SHOT_REGION", "MATCH_ID",
    "MATCH_DATE", "MATCH_FORMAT", "TOURNAMENT_NAME", "SEASON",
    "MATCH_STAGE", "TOSS_DECISION", "VENUE_ID", "VENUE_NAME",
    "VENUE_CITY", "VENUE_COUNTRY", "BATTING_TEAM_ID",
    "BATTING_TEAM_NAME", "BOWLING_TEAM_ID", "BOWLING_TEAM_NAME",
    "BATSMAN_ID", "BATSMAN_NAME", "BATSMAN_DOB",
    "BATSMAN_NATIONALITY", "BATTING_HAND", "BATSMAN_ROLE",
    "BATSMAN_EMAIL", "BATSMAN_PHONE", "BATSMAN_ICC_RATING",
    "BOWLER_ID", "BOWLER_NAME", "BOWLER_NATIONALITY",
    "BOWLING_STYLE", "BOWLER_ICC_RATING", "CONTRACT_ID",
    "CONTACT_GRADE", "ANNUAL_RETAINER_USD", "CONTRACT_STATUS",
    "DRS_REVIEW_ID", "DRS_REVIEWED_BY", "DRS_ORIGINAL_DECISION",
    "DRS_OUTCOME",
)


def load_new_csv_files():
    files = sorted(INPUT_FOLDER.glob("*.csv"))

    if not files:
        print("No CSV files found. Nothing to ingest.")
        return

    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for file in files:
                if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv", file.name):
                    raise ValueError(f"Invalid CSV filename: {file.name}")

                cursor.execute(
                    """
                    SELECT COUNT(*)
                    FROM BRONZE.FILE_AUDIT
                    WHERE FILENAME = %s
                      AND UPPER(STATUS) = 'SUCCESS'
                    """,
                    (file.name,),
                )

                if cursor.fetchone()[0] > 0:
                    print(f"Already processed; skipping {file.name}")
                    continue

                # Upload the complete file once.
                cursor.execute(
                    f"PUT 'file://{file.resolve().as_posix()}' "
                    "@BRONZE.MY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )

                staged_file = file.name + ".gz"
                name_literal = file.name.replace("'", "''")

                target_columns = ", ".join(
                    (*CSV_COLUMNS, "SOURCEFILE")
                )
                source_columns = ", ".join(
                    f"t.${i}" for i in range(1, len(CSV_COLUMNS) + 1)
                )

                try:
                    # Clean up rows left by an earlier incomplete attempt
                    # for this filename, then reload the whole file.
                    cursor.execute(
                        """
                        DELETE FROM BRONZE.RAW_CRICKET
                        WHERE SOURCEFILE = %s
                        """,
                        (file.name,),
                    )

                    cursor.execute(
                        f"""
                        COPY INTO BRONZE.RAW_CRICKET ({target_columns})
                        FROM (
                            SELECT {source_columns},
                                   '{name_literal}'
                            FROM @BRONZE.MY_STAGE/{staged_file} t
                        )
                        FILE_FORMAT = (
                            FORMAT_NAME = 'BRONZE.CSV_FILE_FORMAT'
                        )
                        ON_ERROR = ABORT_STATEMENT
                        FORCE = TRUE
                        """
                    )

                    cursor.execute(
                        """
                        SELECT COUNT(*)
                        FROM BRONZE.RAW_CRICKET
                        WHERE SOURCEFILE = %s
                        """,
                        (file.name,),
                    )
                    loaded_rows = cursor.fetchone()[0]

                    # Record success only after the load and count check.
                    # The expected count is obtained from the CSV data rows.
                    import csv
                    with file.open(
                        encoding="utf-8-sig", newline=""
                    ) as source:
                        expected_rows = sum(
                            1 for _ in csv.reader(source)
                        ) - 1

                    if loaded_rows != expected_rows:
                        raise ValueError(
                            f"{file.name}: loaded {loaded_rows} rows; "
                            f"expected {expected_rows}"
                        )

                    cursor.execute(
                        """
                        INSERT INTO BRONZE.FILE_AUDIT
                            (FILENAME, ROWSLOADED, STATUS)
                        VALUES (%s, %s, 'SUCCESS')
                        """,
                        (file.name, loaded_rows),
                    )
                    conn.commit()
                    print(
                        f"Loaded {loaded_rows} rows from {file.name}"
                    )

                except Exception as exc:
                    conn.rollback()
                    cursor.execute(
                        """
                        INSERT INTO BRONZE.FILE_AUDIT
                            (FILENAME, ROWSLOADED, STATUS)
                        VALUES (%s, 0, %s)
                        """,
                        (file.name, f"FAILED: {str(exc)[:450]}"),
                    )
                    conn.commit()
                    raise


with DAG(
    dag_id="cricket_incremental_copy_dbt",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_tasks=2,
    tags=["snowflake", "dbt", "cricket", "incremental"],
) as dag:

    load_bronze = PythonOperator(
        task_id="load_new_csv_files",
        python_callable=load_new_csv_files,
    )

    dbt_transform = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_PATH,
        ),
        profile_config=ProfileConfig(
            profile_name="capstone_dbt",
            target_name="dev",
            profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
        ),
    )

    load_bronze >> dbt_transform
