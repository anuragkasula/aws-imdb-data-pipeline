from datetime import datetime
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

REGION = os.getenv("AWS_DEFAULT_REGION","us-east-1")
ATHENA_DB = os.getenv("ATHENA_DB","imdb_processed_db")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT","s3://imdb-data-raw-ak/athena-results/")

with DAG(
    dag_id="imdb_smoke_pipeline",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
) as dag:

    ge_validate = BashOperator(
        task_id="ge_validate",
        bash_command="python /opt/airflow/repo/tests/ge_validate_athena.py",
        env={"AWS_REGION": REGION, "ATHENA_DB": ATHENA_DB, "ATHENA_OUTPUT": ATHENA_OUTPUT},
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/repo/imdb_analytics && dbt run --profiles-dir /opt/airflow/repo/.dbt",
        env={"DBT_PROFILES_DIR": "/opt/airflow/repo/.dbt"},
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/repo/imdb_analytics && dbt test --profiles-dir /opt/airflow/repo/.dbt",
        env={"DBT_PROFILES_DIR": "/opt/airflow/repo/.dbt"},
    )

    ge_validate >> dbt_run >> dbt_test