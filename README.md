# 🎬 AWS IMDb Data Pipeline with AI, Airflow, Tableau, and CI/CD

This project is a hands-on, end-to-end Data Engineering pipeline using AWS (S3, Glue, Athena), orchestrated with Docker-based Airflow, and enhanced with an AI genre recommender system. Data is visualized in Tableau or Streamlit. CI/CD is automated with GitHub Actions. All AWS services are kept under $20/month.

---

## 🎯 Project Goals

- Build a cloud-native data pipeline (AWS S3, Glue, Athena)
- Orchestrate ETL with Airflow (Docker)
- Apply AI to recommend movie genres
- Visualize data using Tableau/Streamlit
- Automate deployments with CI/CD (GitHub Actions)

## 🧱 Tech Stack

- Python, IMDb Datasets
- AWS S3 (storage)
- AWS Glue (ETL, PySpark)
- AWS Athena (querying)
- Apache Airflow (Docker)
- Tableau Public / Streamlit (visualization)
- GitHub Actions (CI/CD)
- AI with TF-IDF/Sentence Transformers

## 🤖 AI: Genre Recommender

Content-based movie recommender using:
- TF-IDF or Sentence-BERT embeddings
- Suggests similar movies for a given title
- Demo in Streamlit or results in Tableau

## 🔁 CI/CD

Automated with GitHub Actions:
- Linting and code style checks
- Deploys Glue scripts, Airflow DAGs
- Manages secrets securely

## 📁 Project Structure

- `dags/` — Airflow DAGs
- `glue_jobs/` — Glue ETL scripts
- `airflow/` — Docker Airflow setup
- `scripts/` — Data ingestion
- `ai/` — Genre recommender code
- `docker/` — Streamlit Docker setup
- `tableau/` — Dashboards
- `architecture/` — Diagrams
- `docs/` — SQL and notes
- `.github/workflows/` — CI/CD workflows

## 🗺️ Architecture Diagram

Diagram will be added in `architecture/`.  
**High-level flow:**  
IMDb data → S3 → Glue → S3 (processed) → Athena → Tableau/AI → Visualization  
Orchestration: Airflow | CI/CD: GitHub Actions

## 📚 IMDb Datasets Used

- title.basics.tsv.gz
- title.ratings.tsv.gz
- title.akas.tsv.gz
- title.principals.tsv.gz  
[IMDb Datasets Download](https://datasets.imdbws.com/)

## 📊 Outputs

- Tableau dashboard: trends, genre stats, recommendations
- Streamlit app: live genre recommendations

## 💰 AWS Cost Control

All services are Free Tier or low-cost.
- S3 < 5GB
- Glue dev jobs
- Athena (Parquet format)
- **Estimated cost: <$20/month**

## 👤 Author

Anurag Kasula
anurag.kasula@gmail.com
