# Variante slim (issue #66): só o core e 9 providers básicos. A imagem completa traz ~80 providers
# (Google, Snowflake, Ray, litellm...) que as DAGs não usam (só BashOperator, PythonOperator e o
# SparkSubmitOperator) e respondiam pela maior parte dos CVEs de pacotes Python da imagem.
FROM apache/airflow:slim-3.3.2-python3.11

USER root

# Atualizações de segurança do SO da base (issue #66): a imagem oficial é gerada uma vez por versão
# e envelhece; sem isto o trivy acusa dezenas de pacotes do SO já corrigidos no repositório da distro.
# O setuptools/wheel do Python do sistema (/usr/local, fora do ambiente do usuário airflow) também.
# msodbcsql18 (driver do SQL Server, que a plataforma não usa) fica na versão da base: atualizá-lo
# exige aceitar a EULA da Microsoft.
RUN apt-mark hold msodbcsql18 \
    && apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /usr/lib/jvm/default-java \
    && /usr/local/bin/python -m pip install --no-cache-dir --upgrade setuptools==84.0.0 wheel==0.48.0

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

# `apache-airflow==` na mesma instalação: sem ele o pip pode trocar dependências do próprio Airflow
# (issue #66). O s3fs acompanha a versão do fsspec da base (os dois são lançados juntos).
# Provider do Spark 5.6.0: aceita o pyspark completo (>= 3.5.2), que traz o spark-submit usado pelo
# SparkSubmitOperator. A 6.x troca a dependência pelo pyspark-client (só Spark Connect, sem
# spark-submit) e conflitaria com o pyspark completo no mesmo ambiente (#69).
# Extra `postgres`: o Airflow 3 também abre o banco de metadados em modo assíncrono (asyncpg), que a
# imagem slim não traz.
RUN pip install --no-cache-dir \
    "apache-airflow[postgres]==3.3.2" \
    apache-airflow-providers-apache-spark==5.6.0 \
    pyspark==4.2.0 \
    psycopg2-binary==2.9.10 \
    pydantic-settings==2.2.1 \
    python-dotenv==1.2.2 \
    loguru==0.7.2 \
    faker==24.0.0 \
    pyarrow==23.0.1 \
    yfinance==0.2.36 \
    s3fs==2026.7.0 \
    great-expectations==0.18.13 \
    pandas==2.2.1 \
    numpy==1.26.4 \
    boto3 \
    setuptools==84.0.0 \
    wheel==0.48.0 \
    urllib3==2.8.0

# O pip do ambiente do usuário airflow (26.2.1) vendoriza msgpack e setuptools com CVE (o trivy lê o
# pip/_vendor/vendor.txt), e a versão mais nova do pip ainda traz os mesmos. O Airflow não usa pip em
# runtime; o pip do sistema (/usr/python) continua disponível para manutenção.
RUN python -m pip uninstall -y pip
