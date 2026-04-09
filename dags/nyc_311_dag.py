from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner":        "nyc_311",
    "retries":      1,
    "retry_delay":  timedelta(seconds=15),
    "start_date":   datetime(2026, 4, 5),
}

with DAG(
    dag_id           = "nyc_311_pipeline",
    default_args     = default_args,
    schedule_interval= None,
    catchup          = False,
    description      = "NYC 311: bronze -> silver -> gold tables -> gold request table -> gold aggregations",
    tags             = ["NYC 311 pipeline"],
) as dag:
    bronze = BashOperator(
        task_id      = "bronze_load",
        bash_command = "cd /opt/airflow/spark && python bronze_load.py",
    )

    silver = BashOperator(
        task_id      = "silver_transform",
        bash_command = "cd /opt/airflow/spark && python silver_transform.py",
    )

    gold_tables = BashOperator(
        task_id      = "gold_tables",
        bash_command = "cd /opt/airflow/spark && python gold_tables.py",
    )

    gold_requests_table = BashOperator(
        task_id      = "gold_requests_table",
        bash_command = "cd /opt/airflow/spark && python gold_requests_table.py",
    )

    gold_agg = BashOperator(
        task_id      = "gold_aggregations",
        bash_command = "cd /opt/airflow/spark && python gold_aggregations.py",
    )

    bronze >> silver >> gold_tables >> gold_requests_table >> gold_agg