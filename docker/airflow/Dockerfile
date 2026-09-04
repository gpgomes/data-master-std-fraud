FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java

# JAVA_HOME resolvido dinamicamente acima (funciona em amd64 e arm64) — o
# SparkSubmitOperator precisa de um JRE local, já que roda spark-submit em
# modo client (driver dentro do próprio container do Airflow).
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

# pyspark traz o binário spark-submit (mesma versão do cluster, pyproject.toml)
ENV PATH="/home/airflow/.local/lib/python3.11/site-packages/pyspark/bin:${PATH}"

# spark-submit roda em modo client: o driver herda o ambiente do processo que
# o lançou (a task do Airflow), então precisa de /opt/airflow no PYTHONPATH
# pra "import src.common.config" resolver dentro de bronze_to_silver.py /
# silver_to_gold.py — mesmo ajuste feito em docker/spark/Dockerfile.
ENV PYTHONPATH="/opt/airflow"

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.7.1 \
    pyspark==3.5.1 \
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
    tenacity==8.2.3 \
    s3fs==2024.3.1 \
    great-expectations==0.18.13