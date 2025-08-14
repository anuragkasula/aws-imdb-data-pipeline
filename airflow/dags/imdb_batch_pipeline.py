# imdb_batch_pipeline.py  (batch ETL → Crawler → QA/dbt)
from __future__ import annotations
import os, time, boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

REGION        = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "etl-movies-episodes-analytics")
CRAWLER_NAME  = os.getenv("CRAWLER_NAME", "imdb-processed-crawler")
ATHENA_DB     = os.getenv("ATHENA_DB", "imdb_processed_db")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://imdb-data-raw-ak/athena-results/")
DBT_PROFILES  = "/opt/airflow/repo/.dbt"
DBT_PROJECT = "/opt/airflow/repo/imdb_analytics"  

def _maybe_ingest_imdb():
    if os.getenv("INGEST_ENABLED", "0") != "1":
        print("[INGEST] Disabled (set INGEST_ENABLED=1 to enable). Continuing…")
        return
    rc = os.system("python /opt/airflow/repo/scripts/imdb_ingest_to_s3.py")
    if rc != 0:
        raise RuntimeError("Ingest script failed")

def _start_glue_and_wait(**context):
    gl = boto3.client("glue", region_name=REGION)
    run_ts = context["ts_nodash"]
    run_date = run_ts[:8]
    context['ti'].xcom_push(key='run_date', value=run_date)
    print(f"[GLUE] run_ts={run_ts} run_date={run_date}")
    run_id = gl.start_job_run(JobName=GLUE_JOB_NAME, Arguments={"--run_ts": run_ts})["JobRunId"]
    print(f"[GLUE] Started job {GLUE_JOB_NAME} run_id={run_id}")
    while True:
        jr = gl.get_job_run(JobName=GLUE_JOB_NAME, RunId=run_id)["JobRun"]
        st = jr["JobRunState"]
        print(f"[GLUE] State={st}")
        if st in {"SUCCEEDED","FAILED","ERROR","STOPPED","TIMEOUT"}:
            if st != "SUCCEEDED":
                raise RuntimeError(f"Glue ended with state {st}")
            break
        time.sleep(10)

def _start_crawler_and_wait():
    gc = boto3.client("glue", region_name=REGION)
    try:
        gc.start_crawler(Name=CRAWLER_NAME)
        print(f"[CRAWLER] Start requested for {CRAWLER_NAME}")
    except gc.exceptions.CrawlerRunningException:
        print(f"[CRAWLER] Already running; will wait…")
    last = None
    while True:
        cr = gc.get_crawler(Name=CRAWLER_NAME)["Crawler"]
        st = cr["State"]
        if st != last:
            print(f"[CRAWLER] State={st}")
            last = st
        if st == "READY":
            lc = cr.get("LastCrawl", {})
            if lc.get("Status") and lc["Status"] != "SUCCEEDED":
                raise RuntimeError(f"Crawler finished with status {lc['Status']}")
            break
        time.sleep(10)

def _athena_smoke_check(min_expected: int = 1):
    ath = boto3.client("athena", region_name=REGION)
    q = f"""
      SELECT count(*) c
      FROM analytics_movie_facts
      WHERE decade='2010' AND genre='Drama'
    """
    qid = ath.start_query_execution(
        QueryString=q,
        QueryExecutionContext={"Database": ATHENA_DB},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup="primary",
        ResultReuseConfiguration={"ResultReuseByAgeConfiguration": {"Enabled": False}},
    )["QueryExecutionId"]
    while True:
        st = ath.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
        if st in {"SUCCEEDED","FAILED","CANCELLED"}:
            if st != "SUCCEEDED":
                raise RuntimeError(f"Athena smoke check failed: {st}")
            break
        time.sleep(2)
    rows = ath.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
    val = int(rows[1]["Data"][0]["VarCharValue"]) if len(rows) > 1 else 0
    print(f"[ATHENA] Rowcount={val}")
    if val < min_expected:
        raise RuntimeError(f"Rowcount too low: {val}")

with DAG(
    dag_id="imdb_batch_pipeline",
    description="IMDb batch ETL → Catalog → QA → dbt",
    start_date=datetime(2025,1,1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "you", "retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["imdb","aws","batch"],
) as dag:

    ingest = PythonOperator(
        task_id="ingest_imdb_optional",
        python_callable=_maybe_ingest_imdb,
        doc_md="Optional raw ingest (off by default).",
    )

    glue_etl = PythonOperator(
        task_id="glue_etl",
        python_callable=_start_glue_and_wait,
        execution_timeout=timedelta(minutes=45),
        trigger_rule=TriggerRule.ALL_DONE,  # run even if ingest was skipped
    )

    crawl = PythonOperator(
        task_id="crawl_processed_catalog",
        python_callable=_start_crawler_and_wait,
        execution_timeout=timedelta(minutes=20),
    )

    with TaskGroup("quality_and_modeling") as qm:
        athena_check = PythonOperator(
            task_id="athena_partition_smoke_check",
            python_callable=_athena_smoke_check,
            op_kwargs={"min_expected": 1},
        )
        ge = BashOperator(
            task_id="great_expectations_validate",
            bash_command="python /opt/airflow/repo/tests/ge_validate_athena.py",
            env={"AWS_REGION": REGION, "ATHENA_DB": ATHENA_DB, "ATHENA_OUTPUT": ATHENA_OUTPUT},
        )
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=f"""
             set -e
             export PATH="$PATH:/home/airflow/.local/bin"
             export DBT_PROFILES_DIR="{DBT_PROFILES}"
             cd "{DBT_PROJECT}"
             dbt deps || true
             dbt run --profiles-dir $DBT_PROFILES_DIR
            """,
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"""
             set -e
             export PATH="$PATH:/home/airflow/.local/bin"
             export DBT_PROFILES_DIR="{DBT_PROFILES}"
             cd "{DBT_PROJECT}"
             dbt test --profiles-dir $DBT_PROFILES_DIR
            """,
        )
        athena_check >> ge >> dbt_run >> dbt_test

    ingest >> glue_etl >> crawl >> qm
