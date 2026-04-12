FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    boto3==1.34.74 \
    psycopg2-binary==2.9.9 \
    pydantic==2.7.1 \
    pydantic-settings==2.2.1 \
    python-dotenv==1.0.1 \
    loguru==0.7.2 \
    faker==24.0.0 \
    pandas==2.2.1 \
    pyarrow==15.0.0 \
    numpy==1.26.4 \
    yfinance==0.2.36 \
    tenacity==8.2.3