# \# 🎬 AWS IMDb Data Pipeline with AI + Airflow + Tableau + CI/CD

# 

# This is a production-style \*\*end-to-end Data Engineering project\*\* using AWS services to ingest, transform, and analyze \*\*IMDb movie \& TV data\*\*, with a built-in \*\*AI-based genre recommender\*\* and \*\*CI/CD automation\*\*.  

# Final visualizations are built in \*\*Tableau\*\* or \*\*Streamlit\*\*.

# 

# ---

# 

# \## 🎯 Project Goals

# 

# \- Build a real-world, cloud-native data pipeline on AWS

# \- Ingest IMDb datasets and transform using AWS Glue

# \- Store and query curated data using Athena

# \- Orchestrate ETL pipeline using Apache Airflow (Docker)

# \- Add an AI-based genre recommendation engine

# \- Visualize trends and recommendations using Tableau

# \- Implement GitHub Actions-based CI/CD

# 

# ---

# 

# \## 🧱 Tech Stack

# 

# | Layer             | Tools                          |

# |-------------------|--------------------------------|

# | \*\*Ingestion\*\*     | Python, IMDb Datasets          |

# | \*\*Storage\*\*       | Amazon S3                      |

# | \*\*ETL/ELT\*\*       | AWS Glue (PySpark)             |

# | \*\*Orchestration\*\* | Apache Airflow (Docker)        |

# | \*\*Querying\*\*      | Amazon Athena                  |

# | \*\*AI/NLP\*\*        | TF-IDF / Sentence Transformers |

# | \*\*BI\*\*            | Tableau Public / Streamlit     |

# | \*\*CI/CD\*\*         | GitHub Actions, AWS CLI        |

# 

# ---

# 

# \## 🤖 AI: Genre Recommender System

# 

# This project includes a content-based \*\*AI recommender system\*\* that suggests similar movies based on plot summaries and metadata.

# 

# \- Techniques: TF-IDF + Cosine Similarity or Sentence-BERT

# \- Output: Top 10 similar movies for a selected title

# \- Interface: Streamlit (local Docker container) or precomputed results in Tableau

# 

# ---

# 

# \## 🔁 CI/CD (GitHub Actions)

# 

# This repo includes automated CI/CD workflows for:

# 

# \- ✅ Linting (Black, Flake8)

# \- ✅ Unit tests for ingestion and AI modules

# \- ✅ Auto-deploy of Glue scripts to S3

# \- ✅ DAG sync to Airflow Docker instance

# \- ✅ Infra-as-code (optional via CDK)

# \- ✅ AWS Secrets secured using GitHub Secrets

# 

# ---

# 

# \## 🗂️ Project Folder Structure

# 

# ```bash

# aws-imdb-data-pipeline/

# ├── dags/                  # Airflow DAGs

# ├── glue\_jobs/             # Glue ETL scripts (PySpark)

# ├── airflow/               # Docker Airflow setup

# ├── scripts/               # IMDb ingestion scripts

# ├── ai/                    # Genre recommender scripts

# ├── docker/                # Streamlit Docker setup

# ├── tableau/               # Dashboard files or screenshots

# ├── architecture/          # Architecture diagram (PNG / .drawio)

# ├── docs/                  # Notes, SQL queries

# └── .github/workflows/     # CI/CD workflow YAMLs



