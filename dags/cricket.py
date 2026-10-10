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


INPUT_FOLDER = Path("/usr/local/airflow/include/data")
DBT_PROJECT_PATH = "/usr/local/airflow/dbt_project"
chunk_rows = 50_000
csv_columns = (
    "DELIVERY_ID","DELIVERY_TIMESTAMP","INNINGS_NO","OVER_NO",
    "BALL_IN_OVER","RUNS_OFF_BAT","EXTRA_TYPE","EXTRA_RUNS",
    "TOTAL_RUNS","IS_WICKET","DISMISSAL_TYPE","BALL_SPEED_KMPH",
    "NON_STRIKER_ID","SHOT_TYPE","SHOT_REGION","MATCH_ID",
    "MATCH_DATE","MATCH_FORMAT","TOURNAMENT_NAME","SEASON",
    "MATCH_STAGE","TOSS_DECISION","VENUE_ID","VENUE_NAME",
    "VENUE_CITY","VENUE_COUNTRY","BATTING_TEAM_ID","BATTING_TEAM_NAME",
    "BOWLING_TEAM_ID","BOWLING_TEAM_NAME","BATSMAN_ID","BATSMAN_NAME",
    "BATSMAN_DOB","BATSMAN_NATIONALITY","BATTING_HAND","BATSMAN_ROLE",
    "BATSMAN_EMAIL","BATSMAN_PHONE","BATSMAN_ICC_RATING","BOWLER_ID",
    "BOWLER_NAME","BOWLER_NATIONALITY","BOWLING_STYLE","BOWLER_ICC_RATING",
    "CONTRACT_ID","CONTACT_GRADE","ANNUAL_RETAINER_USD","CONTRACT_STATUS",
    "DRS_REVIEW_ID","DRS_REVIEWED_BY","DRS_ORIGINAL_DECISION","DRS_OUTCOME"
)

def load_csv_files() :
    files = sorted(INPUT_FOLDER.glob("*.csv"))
    if not files:
        raise FileNotFoundError(f"Put a CSV in {INPUT_FOLDER} before triggering this DAG")
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor :
            for file in files :
                if not re.fullmatch(r"[A-Za-z0-9_.-]+\.csv",file.name):
                    raise ValueError(f"filename error: {file.name}")
                cursor.execute(
                    "select count(*) from bronze.file_audit "
                    "where FILENAME = %s and status = 'SUCCESS'",
                    (file.name,),
                )
                if cursor.fetchone()[0] :
                    continue
                
                name_literal = file.name.replace("'","''")
                target_columns=", ".join((*csv_columns,"SOURCEFILE"))
                source_columns = ", ".join(
                    f"t.${position}" for position in range(1,len(csv_columns) + 1)
                )
                try :
                    with file.open(encoding="utf-8-sig",newline="") as source :
                        reader=csv.reader(source)
                        header=next(reader)
                        print(reader)
                        print(header)
                        # if [name.lower() for name in csv_columns] != header :
                        # if [name.lower() for name in csv_columns] != [x for x in header] :
                        #     print(name.lower(),header)
                        #     raise ValueError("invalid col name",file.name)

                        cursor.execute(
                            "DELETE FROM BRONZE.RAW_CRICKET "
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
                                        if len(row) != len(csv_columns):
                                            raise ValueError(
                                                f"Bad column count in {file.name}"
                                            )
                                        writer.writerow(row)
                                        chunk_rows += 1
                                        if chunk_rows == chunk_rows:
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
                                    f"COPY INTO BRONZE.RAW_CRICKET "
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
                        "select count(*) from BRONZE.RAW_CRICKET "
                        "where SOURCEFILE = %s",
                        (file.name,),
                    )        
                    count=cursor.fetchone()[0]
                    if count!=expected_rows:
                        raise ValueError("loaded rows:",count,"expected rows:",expected_rows,"from",file.name)

                    cursor.execute(
                        "insert into BRONZE.FILE_AUDIT "
                        "(FILE_NAME, ROWSLOADED, STATUS) values (%s,%s,'success')",
                        (file.name,count),
                    )
                    conn.commit()
                except Exception as e :
                    conn.rollback()
                    cursor.execute(
                        "insert into BRONZE.FILE_AUDIT "
                        "(FILENAME,ROWSLOADED,STATUS) VALUES (%s,0,%s)",
                        (file.name,f"FAILED: {str(e)[:450]}")
                    )
                    conn.commit()
                    raise



with DAG(
    dag_id = "cricket_data_ingestion",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    schedule=None,
    max_active_tasks=2,
    tags=["snowflake", "dbt", "cricket"]
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
    load_bronze  >> dbt_transform

