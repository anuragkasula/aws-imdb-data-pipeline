# airflow/Dockerfile
FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*
USER airflow

# Install Python libs the DAG will use
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir \
        dbt-core==1.9.0 \
        dbt-athena-community==1.9.4 \
        great_expectations==0.18.12 \
        pandas==2.2.2 \
        pyathena==3.9.0 \
        boto3 \
		kafka-python