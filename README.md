# 🎬 IMDb Data Platform

> **Production-grade, end‑to‑end data & analytics project** turning raw IMDb datasets into a Netflix‑styled Power BI experience.  
> Pipeline: **AWS (S3/Glue/Athena)** → **Airflow** orchestrates → **Snowflake (MARTS → ANALYTICS via dbt)** → **Power BI** (Import).

---

## 🔗 Demo & Screens

- **Power BI Report**: (click on the below image to open the interactive dashboard) [![Open the interactive dashboard](docs/pbi-cover.png)](https://app.fabric.microsoft.com/view?r=eyJrIjoiNzk0M2M4ZDktYzJiNy00NGUwLWFlMjAtYzBjZDc1OWNmNTEwIiwidCI6ImNkZjI0MzE5LWQ3NDAtNDNkYS05MTU5LTkyOGQxNzVkMjFlZSJ9)  


**Dashboard summary:**  
A dark, Netflix‑themed report for **Movies & TV Series** with:
- Global slicers (Year, Genre, Series) and a **disambiguating “Title (Year)”** picker
- **KPIs**: Avg Rating ★, Binge Score, Pilot ★, Finale ★, Finale Δ vs Avg (all with zero‑fallback)
- **Movies Explorer**: Top/trending titles, genre share by decade, drill tooltips
- **Series Explorer**: Season→Episode drill lines, heatmap (S×E), best/worst episode callouts
- Interactions tuned for **Filter** (not Highlight) to avoid confusing dual values

---

## 🧭 Architecture (high level)

```mermaid
flowchart LR
  %% --- styles (optional) ---
  classDef svc fill:#111827,color:#ffffff,stroke:#f43f5e,stroke-width:1px;
  classDef store fill:#0e7490,color:#ffffff,stroke:#0ea5e9,stroke-width:1px;
  classDef wh fill:#1f2937,color:#ffffff,stroke:#60a5fa,stroke-width:1px;
  classDef bi fill:#111827,color:#ffffff,stroke:#f59e0b,stroke-width:1px;

  %% --- AWS side ---
  subgraph AWS["AWS"]
    AIRFLOW["Airflow DAG (orchestration)"]:::svc
    GLUE["AWS Glue ETL (PySpark)"]:::svc
    ATHENA["Athena + Great Expectations (Data Quality)"]:::svc
    S3RAW["S3 bucket: imdb-data-raw"]:::store
    S3PROC["S3 bucket: imdb/processed (partitioned by run_date/decade/genre)"]:::store
  end

  %% --- Snowflake side ---
  subgraph SNOWFLAKE["Snowflake"]
    STAGE["External Stage: @IMDB_S3_STAGE"]:::wh
    COPYM["COPY INTO MARTS.* (idempotent load)"]:::wh
    DBT["dbt models → ANALYTICS.* (star schema)"]:::wh
  end

  PBI["Power BI (Import/Direct; Netflix-style dashboards)"]:::bi

  %% --- Flow ---
  AIRFLOW --> GLUE
  GLUE --> S3PROC
  S3RAW --> GLUE
  AIRFLOW --> ATHENA
  ATHENA --> AIRFLOW
  S3PROC --> STAGE
  AIRFLOW --> COPYM
  STAGE --> COPYM
  COPYM --> DBT
  DBT --> PBI
```

**Key design points**
- Glue writes **partitioned parquet** (e.g., decade/genre for movies) and **run_date** for idempotent loads.
- Snowflake loads **only the latest run_date slice**, then **dbt** creates clean **ANALYTICS** tables.
- Power BI uses **Import mode** (fast, predictable for public embeds).

---

## 🧱 Tech Stack

| Layer | Tools |
|---|---|
| Ingestion | Python, IMDb dataset dumps |
| Storage | **Amazon S3** (raw → processed parquet) |
| Transform | **AWS Glue (PySpark)** |
| Quality | **Great Expectations** (in-Glue + Athena checks) |
| Orchestration | **Apache Airflow** (Docker) |
| Warehouse | **Snowflake** (MARTS landing, ANALYTICS models) |
| Modeling | **dbt** (materialized: table / optional incremental) |
| BI | **Power BI** (Import, Netflix theme, measures) |
| Logging | CloudWatch (Glue), Airflow logs |

---

## 📦 Repository Layout

```
repo-root/
├─ dags/                     # Airflow DAGs (imdb_raw_ingest.py, imdb_batch_pipeline.py)
├─ glue_jobs/                # Glue ETL (etl_movies_episodes_analytics_advanced.py)
├─ dbt/
│  ├─ models/marts/          # *_anl.sql (movies, episodes, season summary)
│  ├─ dbt_project.yml
│  └─ profiles/              # profiles.yml (.env for SNOWFLAKE_*)
├─ docs/                     # Screenshots, notes, SQL snippets
├─ docker/                   # Airflow docker-compose & configs
├─ sql/                      # Snowflake DDLs (stages, file formats, tables)
└─ powerbi/                  # Measures (.dax), theme.json, template (.pbit)
```

---

## 🚚 Data Flow (step by step)

1) **Ingest & land (S3/raw)**  
   Download IMDb dumps (TSV) to `s3://.../raw/`.

2) **Transform in Glue (PySpark)**  
   - Type casting, **genre explode**, computed **decade**, top actors, pilot/finale indicators, etc.
   - **Data Quality snapshots** (row counts, null checks) written alongside processed data.
   - Output partitioned parquet in `s3://.../processed/analytics_*` with **run_date**.

3) **Orchestrate in Airflow (Docker)**  
   DAG: `athena_check → ge → snowflake_load → dbt_run → dbt_test`  
   - **snowflake_load**: sets `RUN_DATE = max(run_date)`, **DELETE** that slice, **INSERT** from external stage (`@IMDB_S3_STAGE/...` using `IMDB_PQ_FF`).

4) **Model with dbt (Snowflake)**  
   - Sources: `MARTS.MOVIE_FACTS`, `MARTS.EPISODE_FACTS`, `MARTS.SERIES_SEASON_SUMMARY`
   - Builds: `ANALYTICS.MOVIE_FACTS_ANL`, `ANALYTICS.EPISODE_FACTS_ANL`, `ANALYTICS.SERIES_SEASON_SUMMARY_ANL`
   - **Dedup logic** by business grain + latest `RUN_DATE`; consistent types & names
   - `dbt test` enforces **not null / unique** constraints on keys

5) **Visualize in Power BI**  
   - Import from Snowflake (`IMDB.ANALYTICS`), relationships: **Dim Series (1 → *) → facts**
   - Netflix theme, curated slicers, **Filter** interactions
   - Measures with **fallbacks** (no blanks): Pilot ★, Finale ★, Best Season ★, Δ vs Avg, Popularity/Binge

---

## 🗃️ Data Model (grains & keys)

- **MOVIE_FACTS_ANL** — grain: **movie × genre**  
  Keys: `MOVIE_KEY`, `GENRE` (genre‑exploded by design)  
  Fields: titles, startYear, decade, runtime, genres, rating, votes, directors/writers, oscarWinner, `RUN_DATE`

- **EPISODE_FACTS_ANL** — grain: **series × season × episode**  
  Keys: `SERIES_KEY`, `SEASONNUMBER`, `EPISODENUMBER`  
  Fields: episodeTitle, episodeYear, rating, votes, flags: `ISPILOT`, `ISFINALE`, `ISSPECIAL`, `RUN_DATE`

- **SERIES_SEASON_SUMMARY_ANL** — grain: **series × season**  
  Keys: `SERIES_KEY`, `SEASONNUMBER`  
  Fields: episodesInSeason, avgSeasonRating, seasonStartYear/EndYear, `RUN_DATE`

- **Dim Series (in PBI or dbt)** — unique by `SERIES_KEY`  
  Columns: `SERIES_KEY`, `SERIESTITLE`, **latest** `SERIESSTARTYEAR`, `Series Label = "Title (Year)"`  
  Relationships: **Dim → EPISODE_FACTS_ANL**, **Dim → SERIES_SEASON_SUMMARY_ANL** (single direction, 1→*)

---

## 🧮 Power BI: key measures (highlights)

> Full measure set: `powerbi/imdb_powerbi_measures.dax`

- **Series – Pilot ★**: prefer **S1E1** with `ISPILOT=1` & rating → fallbacks (any S1 pilot, S1E1 rated, earliest rated) → **0**.
- **Series – Finale ★ (Active)**: latest rated; if not a finale then try season finale; else latest rated → **0**.
- **Series – Best Season ★**: max season avg for active series → **0**.
- **Finale Δ vs Avg**: finale − average season rating (both COALESCE’d to 0).

UX choices: **Filter** (not Highlight), color scale red→green, tooltips with `SxEy`, single-select **Series slicer** using **“Title (Year)”**.

---

## ⚙️ Getting Started (quick path)

### 1) Configure env
- AWS credentials configured for S3/Glue/Athena
- Snowflake account & role with usage on `IMDB` DB, `IMDB_WH`, stages/FF
- Docker for Airflow

Create `dbt/profiles/.env` (example):
```bash
SNOWFLAKE_ACCOUNT=your_acct
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_pass
SNOWFLAKE_ROLE=IMDB_ROLE
SNOWFLAKE_WAREHOUSE=IMDB_WH
SNOWFLAKE_DATABASE=IMDB
SNOWFLAKE_SCHEMA=ANALYTICS
```

### 2) Bring up Airflow
```bash
docker compose up -d
# Add connection: snowflake_imdb (Airflow UI) for the SnowflakeOperator / SQLExecuteQueryOperator
```

### 3) Glue ETL
- Upload `glue_jobs/etl_movies_episodes_analytics_advanced.py` to your Glue job.
- Set job params for S3 paths (`raw`, `processed`), run job.
- Verify processed parquet partitions in `s3://.../processed/...`.

### 4) Snowflake objects (FF + Stage)
```sql
USE DATABASE IMDB; USE SCHEMA MARTS;

CREATE OR REPLACE FILE FORMAT IMDB_PQ_FF TYPE=PARQUET;
CREATE OR REPLACE STAGE IMDB_S3_STAGE
  URL='s3://<your-bucket>/processed/'
  STORAGE_INTEGRATION=<your_integration>
  FILE_FORMAT=IMDB_PQ_FF;
```

### 5) Run the DAG
- Trigger `imdb_batch_pipeline` (Airflow).  
  It will: athena_check → GE → snowflake_load (DELETE+INSERT latest run_date) → dbt run → dbt test.

### 6) Build the report
- Power BI Desktop → Snowflake connector → `IMDB.ANALYTICS`
- Create **Dim Series** (or fetch from dbt), set relationships, paste measures/theme.
- Publish to workspace (Import mode). For public share, use **Publish to web** (review your org’s policy).

---

## 📈 CI/CD (optional)
- GitHub Actions for **style/tests**, Glue script sync, and DAG sync to Docker Airflow.
- Optionally call Power BI REST API to **trigger dataset refresh** post‑pipeline.

---

## 📝 Notes
- This README replaces/expands an earlier version focused on AWS + Tableau/Streamlit; we now target **Power BI** and **Snowflake** as the BI/warehouse defaults.
