# imdb_batch_pipeline.py  (batch ETL → Crawler → QA/dbt)
from __future__ import annotations
import os, time, boto3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.state import DagRunState
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

REGION        = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "etl-movies-episodes-analytics")
CRAWLER_NAME  = os.getenv("CRAWLER_NAME", "imdb-processed-crawler")
ATHENA_DB     = os.getenv("ATHENA_DB", "imdb_processed_db")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT", "s3://imdb-data-raw-ak/athena-results/")
DBT_PROFILES  = "/opt/airflow/dags/dbt/profiles"
DBT_PROJECT = "/opt/airflow/dags/dbt"  


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
      FROM analytics_movie_facts_v2
      WHERE run_date = '{run_date}'
      LIMIT 1
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

    raw_ingest = TriggerDagRunOperator(
        task_id="raw_ingest",
        trigger_dag_id="imdb_raw_ingest",           
        conf={"force": True},
        wait_for_completion=False,                   
        poke_interval=30,                           # how often to check the child’s status
        reset_dag_run=True,                         
        logical_date ="{{ ts }}",
    )
    
    wait_for_raw = ExternalTaskSensor(
        task_id="wait_for_raw",
        external_dag_id="imdb_raw_ingest",
        external_task_id="finalize",   # this is the last task in imdb_raw_ingest
        mode="reschedule",             # frees the worker between pokes
        poke_interval=30,
        timeout=60*60,                 
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],  # treat child failure as failure
    )
    
    glue_etl = PythonOperator(
        task_id="glue_etl",
        python_callable=_start_glue_and_wait,
        execution_timeout=timedelta(minutes=45),
        trigger_rule=TriggerRule.ALL_SUCCESS, 
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
             
             set -a
             [ -f "$DBT_PROFILES_DIR/.env" ] && . "$DBT_PROFILES_DIR/.env"
             set +a
             
             export SNOWFLAKE_ROLE=IMDB_ROLE
             export SNOWFLAKE_WAREHOUSE=IMDB_WH
             export SNOWFLAKE_DATABASE=IMDB
             export SNOWFLAKE_SCHEMA=ANALYTICS
             
             echo "DBG env -> role=$SNOWFLAKE_ROLE wh=$SNOWFLAKE_WAREHOUSE db=$SNOWFLAKE_DATABASE schema=$SNOWFLAKE_SCHEMA"
             dbt deps || true
             dbt debug --profiles-dir "$DBT_PROFILES_DIR" -t prod -x   # prints resolved role/db/schema
             dbt run   --profiles-dir "$DBT_PROFILES_DIR" -t prod --threads 4
             """,
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=f"""
             set -e
             export PATH="$PATH:/home/airflow/.local/bin"
             export DBT_PROFILES_DIR="{DBT_PROFILES}"
             cd "{DBT_PROJECT}"
             
             set -a
             [ -f "$DBT_PROFILES_DIR/.env" ] && . "$DBT_PROFILES_DIR/.env"
             set +a
             
             export SNOWFLAKE_ROLE=IMDB_ROLE
             export SNOWFLAKE_WAREHOUSE=IMDB_WH
             export SNOWFLAKE_DATABASE=IMDB
             export SNOWFLAKE_SCHEMA=ANALYTICS
             
             dbt test --profiles-dir "$DBT_PROFILES_DIR" -t prod --threads 4
            """,
        )
        snowflake_setup = SnowflakeOperator(
            task_id="snowflake_setup",
            snowflake_conn_id="snowflake_imdb",
            sql=[
                "USE DATABASE IMDB; USE SCHEMA MARTS;",
                "ALTER TABLE IF EXISTS MOVIE_FACTS            CLUSTER BY (RUN_DATE, DECADE, GENRE);",
                "ALTER TABLE IF EXISTS EPISODE_FACTS          CLUSTER BY (RUN_DATE, SERIES_DECADE, SEASONNUMBER);",
                "ALTER TABLE IF EXISTS SERIES_SEASON_SUMMARY  CLUSTER BY (RUN_DATE, SERIES_DECADE);",
            ],
        )
        snowflake_load = SnowflakeOperator(
            task_id="snowflake_load",
            snowflake_conn_id="snowflake_imdb",  # set this in Airflow Connections
            hook_params={
                "warehouse": "IMDB_WH",
                "database": "IMDB",
                "schema": "MARTS",
                "role": "IMDB_ROLE", 
                "session_parameters": {"QUERY_TAG": "imdb_airflow_dag: {{ ds_nodash }}"},
            },
            sql=[
              # 1) Use the run_date that glue_etl already pushed to XCom.
              "SET RUN_DATE = '{{ ti.xcom_pull(task_ids=\"glue_etl\", key=\"run_date\") }}';",

              # Safety: fail fast if RUN_DATE is invalid/missing
                  "SELECT TO_DATE($RUN_DATE,'YYYYMMDD');",
              
              # 2) MOVIES
              "DELETE FROM MOVIE_FACTS WHERE RUN_DATE = $RUN_DATE;",
              r"""
              INSERT INTO MOVIE_FACTS (
                TCONST, PRIMARYTITLE, ORIGINALTITLE, STARTYEAR, DECADE, RUNTIMEMINUTES,
                GENRE, GENRES, AVERAGERATING, NUMVOTES, DIRECTORS, WRITERS,
                TOPACTORS, TOPACTORS_NCONST, OSCARWINNER, RUN_DATE, RUN_DATE_DT
              )
              SELECT
                $1:"tconst"::string,
                $1:"primaryTitle"::string,
                $1:"originalTitle"::string,
                TRY_TO_NUMBER(NULLIF($1:"startYear"::string,'\\N')),
                TRY_TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME,'decade=(\\d+)',1,1,'e',1)),
                TRY_TO_NUMBER(NULLIF($1:"runtimeMinutes"::string,'\\N')),
                REGEXP_SUBSTR(METADATA$FILENAME,'genre=([^/]+)',1,1,'e',1),
                $1:"genres"::string,
                TRY_TO_DOUBLE(NULLIF($1:"averageRating"::string,'\\N')),
                TRY_TO_NUMBER(NULLIF($1:"numVotes"::string,'\\N')),
                $1:"directors"::string,
                $1:"writers"::string,
                $1:"topActors"::string,
                $1:"topActors_nconst"::string,
                TRY_TO_NUMBER(NULLIF($1:"oscarWinner"::string,'\\N')),
                $RUN_DATE, TO_DATE($RUN_DATE,'YYYYMMDD')
              FROM @IMDB.MARTS.IMDB_S3_STAGE/analytics_movie_facts_v2/run_date=$RUN_DATE/
                   (FILE_FORMAT => IMDB.MARTS.IMDB_PQ_FF);
              """,

              # 3) EPISODES
              "DELETE FROM EPISODE_FACTS WHERE RUN_DATE = $RUN_DATE;",
              r"""
              INSERT INTO EPISODE_FACTS (
                TCONST, SERIESID, SERIESTITLE, SERIESSTARTYEAR, SERIES_DECADE,
                SEASONNUMBER, EPISODENUMBER, EPISODETITLE, EPISODEYEAR,
                AVERAGERATING, NUMVOTES, ISPILOT, ISFINALE, ISSPECIAL, RUN_DATE, RUN_DATE_DT
              )
              SELECT
                $1:"tconst"::string,
                $1:"seriesId"::string,
                $1:"seriesTitle"::string,
                TRY_TO_NUMBER(NULLIF($1:"seriesStartYear"::string,'\\N')),
                TRY_TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME,'series_decade=(\\d+)',1,1,'e',1)),
                COALESCE(
                  TRY_TO_NUMBER(NULLIF($1:"seasonNumber"::string,'\\N')),
                  TRY_TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME,'seasonNumber=(-?\\d+)',1,1,'e',1))
                ),
                TRY_TO_NUMBER(NULLIF($1:"episodeNumber"::string,'\\N')),
                $1:"episodeTitle"::string,
                TRY_TO_NUMBER(NULLIF($1:"episodeYear"::string,'\\N')),
                TRY_TO_DOUBLE(NULLIF($1:"averageRating"::string,'\\N')),
                TRY_TO_NUMBER(NULLIF($1:"numVotes"::string,'\\N')),
                COALESCE(TRY_TO_NUMBER($1:"isPilot"::string),
                         IFF(LOWER($1:"isPilot"::string)='true',1,0)),
                COALESCE(TRY_TO_NUMBER($1:"isFinale"::string),
                         IFF(LOWER($1:"isFinale"::string)='true',1,0)),
                COALESCE(TRY_TO_NUMBER($1:"isSpecial"::string),
                         IFF(LOWER($1:"isSpecial"::string)='true',1,0)),
                $RUN_DATE, TO_DATE($RUN_DATE,'YYYYMMDD')
              FROM @IMDB.MARTS.IMDB_S3_STAGE/analytics_episode_facts_v2/run_date=$RUN_DATE/
                   (FILE_FORMAT => IMDB.MARTS.IMDB_PQ_FF);
              """,

              # 4) SEASONS
              "DELETE FROM SERIES_SEASON_SUMMARY WHERE RUN_DATE = $RUN_DATE;",
              r"""
              INSERT INTO SERIES_SEASON_SUMMARY (
                SERIESID, SERIESTITLE, SERIES_DECADE, SEASONNUMBER,
                EPISODESINSEASON, AVGSEASONRATING, SEASONSTARTYEAR, SEASONENDYEAR,
                RUN_DATE, RUN_DATE_DT
              )
              SELECT
                $1:"seriesId"::string,
                $1:"seriesTitle"::string,
                TRY_TO_NUMBER(REGEXP_SUBSTR(METADATA$FILENAME,'series_decade=(\\d+)',1,1,'e',1)),
                TRY_TO_NUMBER(NULLIF($1:"seasonNumber"::string,'\\N')),
                TRY_TO_NUMBER(NULLIF($1:"episodesInSeason"::string,'\\N')),
                TRY_TO_DOUBLE(NULLIF($1:"avgSeasonRating"::string,'\\N')),
                TRY_TO_NUMBER(NULLIF($1:"seasonStartYear"::string,'\\N')),
                TRY_TO_NUMBER(NULLIF($1:"seasonEndYear"::string,'\\N')),
                $RUN_DATE, TO_DATE($RUN_DATE,'YYYYMMDD')
              FROM @IMDB.MARTS.IMDB_S3_STAGE/series_season_summary_v2/run_date=$RUN_DATE/
                   (FILE_FORMAT => IMDB.MARTS.IMDB_PQ_FF);
              """,
            ]
,
        )
        athena_check >> ge >> snowflake_setup >> snowflake_load >> dbt_run >> dbt_test  

    raw_ingest >> wait_for_raw >> glue_etl >> crawl >> qm
