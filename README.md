# \# 🎬 AWS IMDb Data Pipeline with AI + Airflow + Tableau + CI/CD

# 

# This is a production-style \*\*end-to-end Data Engineering project\*\* using AWS services to ingest, transform, and analyze \*\*IMDb movie \& TV data\*\*, with a built-in \*\*AI-based genre recommender\*\* and \*\*CI/CD automation\*\*. Final visualizations are built in \*\*Tableau\*\* or \*\*Streamlit\*\*.

# 

# ---

# 

# \## 🎯 Project Goals

# 

# \- Build a real-world, cloud-native data pipeline on AWS

# \- Ingest IMDb datasets and transform using AWS Glue

# \- Query using Athena and visualize in Tableau

# \- Create a movie \*\*genre recommender\*\* using Python NLP

# \- Use Docker + Airflow to orchestrate all stages

# \- Deploy with GitHub Actions (CI/CD)

# \- Stay under \*\*$20/month AWS usage\*\*

# 

# ---

# 

# \## 🧱 Tech Stack

# 

# | Layer | Tools |

# |-------|-------|

# | \*\*Data Ingestion\*\* | Python, IMDb Datasets |

# | \*\*Cloud Storage\*\* | Amazon S3 |

# | \*\*ETL/ELT\*\* | AWS Glue (PySpark) |

# | \*\*Orchestration\*\* | Apache Airflow (Docker) |

# | \*\*Query Engine\*\* | Amazon Athena |

# | \*\*AI Module\*\* | TF-IDF / Sentence Transformers |

# | \*\*Visualization\*\* | Tableau Public OR Streamlit |

# | \*\*CI/CD\*\* | GitHub Actions + AWS CLI |

# | \*\*Infra as Code (IAC)\*\* | AWS CDK or CloudFormation (optional) |

# 

# ---

# 

# \## 🧠 AI Integration: Genre Recommender

# 

# This project includes a lightweight \*\*AI module\*\* that recommends similar movies based on genre, plot, keywords, and metadata.

# 

# \### How It Works:

# \- Input: A movie title (e.g., \*Inception\*)

# \- Output: List of similar movies

# \- Method: 

# &nbsp; - TF-IDF + Cosine Similarity

# &nbsp; - OR Sentence-BERT (via HuggingFace)

# \- Output visualized in Streamlit or Tableau

# 

# ---

# 

# \## 🔁 CI/CD Pipeline

# 

# \*\*GitHub Actions\*\* automates:

# \- ✅ Code formatting checks (Black, Flake8)

# \- ✅ Unit tests for ingestion and recommender logic

# \- ✅ Auto-deploy of Glue scripts to S3

# \- ✅ DAG sync to Airflow directory

# \- ✅ Infra deployment via AWS CDK (optional)

# \- ✅ Secrets managed via GitHub Encrypted Secrets

# 

# ---

# 

# \## 📁 Project Structure

# 

# aws-imdb-data-pipeline/

# │

# ├── dags/ # Airflow DAGs

# ├── glue\_jobs/ # AWS Glue scripts (PySpark)

# ├── airflow/ # Docker Airflow setup

# ├── scripts/ # IMDb ingestion scripts

# ├── ai/ # Genre recommender scripts

# ├── docker/ # Dockerfiles for Streamlit/other services

# ├── tableau/ # Dashboards or screenshots

# ├── architecture/ # Architecture diagram

# ├── docs/ # SQL queries, notes

# └── .github/workflows/ # GitHub Actions YAMLs

# 

# 

# 

# ---

# 

# \## 🗺️ Architecture Diagram

# 

# > Coming soon — will be placed in `/architecture/`  

# Diagram includes:

# \- IMDb → Python → S3 (Raw Zone)

# \- AWS Glue → S3 (Processed)

# \- Athena → Tableau

# \- AI (Recommender) → Streamlit

# \- Orchestration via Airflow

# \- CI/CD via GitHub Actions

# 

# ---

# 

# \## ✅ Status Tracker

# 

# | Feature | Status |

# |---------|--------|

# | 📦 GitHub Repo + Folder Setup | ✅ Completed |

# | 📥 IMDb Data Ingestion         | ⏳ In Progress |

# | 🔄 Glue Transformation Jobs    | ❌ Pending |

# | 🔍 Athena Query Setup          | ❌ Pending |

# | 🧠 Genre Recommender AI        | ❌ Pending |

# | 📊 Tableau Dashboard           | ❌ Pending |

# | ⚙️ Airflow Orchestration       | ❌ Pending |

# | 🔁 CI/CD Integration           | ❌ Pending |

# | 📈 Streamlit Demo (Optional)   | ❌ Pending |

# 

# ---

# 

# \## 💰 AWS Cost Estimate

# 

# \- \*\*AWS Free Tier eligible services used\*\*:

# &nbsp; - Glue (monthly dev endpoint \& jobs)

# &nbsp; - S3 (storage < 5 GB)

# &nbsp; - Athena (pay per query → keep data in Parquet)

# \- Total cost expected: \*\*<$20/month\*\*

# 

# ---

# 

# \## 📚 IMDb Data Reference

# 

# We use the official IMDb datasets:  

# 📎 \[IMDb Datasets Download](https://datasets.imdbws.com/)  

# \- `title.basics.tsv.gz`  

# \- `title.ratings.tsv.gz`  

# \- `title.akas.tsv.gz`  

# \- `title.principals.tsv.gz`

# 

# ---

# 

# \## 🧠 Topics You'll Master

# 

# \- AWS S3, Glue, Athena, IAM

# \- PySpark ETL logic

# \- Orchestration with Airflow DAGs

# \- NLP techniques for recommender systems

# \- GitHub Actions for automated deployment

# \- Tableau storytelling from curated data

# 

# ---

# 

# \## 👨‍💻 Author

# 

# Made with 💻 by Anurag  





