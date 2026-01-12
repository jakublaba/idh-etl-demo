FROM quay.io/astronomer/astro-runtime:13.3.0

WORKDIR /usr/local/airflow

# Copy credentials and data
COPY gcp-credentials.json .
COPY data ./data

# DuckDB data directory
RUN mkdir -p /usr/local/airflow/duckdb

VOLUME ["/usr/local/airflow/duckdb"]
