"""Trigger this DAG to check the Snowflake connection."""

from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(
    dag_id="test_snowflake_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "test"],
) as dag:
    SQLExecuteQueryOperator(
        task_id="test_snowflake_ping",
        conn_id="snowflake_default",
        sql="SELECT CURRENT_VERSION(), CURRENT_ACCOUNT(), CURRENT_ROLE(), "
        "CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE()",
    )
