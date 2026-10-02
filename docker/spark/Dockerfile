FROM apache/spark:4.2.0

USER root

# Atualizações de segurança do SO da base (issue #66): a imagem oficial é gerada uma vez por versão
# e envelhece; sem isto o trivy acusa dezenas de pacotes do SO já corrigidos no repositório da distro.
RUN apt-get update && apt-get upgrade -y --no-install-recommends && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir \
    boto3==1.34.74 \
    s3fs==2024.3.1 \
    pydantic==2.12.5 \
    pydantic-settings==2.2.1 \
    python-dotenv==1.2.2 \
    loguru==0.7.2 \
    psycopg2-binary==2.9.10

# S3A (MinIO): o hadoop-aws tem de ser da mesma versão do Hadoop da distribuição (3.5.0 no Spark 4.2),
# e desde o Hadoop 3.4 ele usa o AWS SDK v2. O SDK v1 (aws-java-sdk-bundle) saiu; a versão do bundle
# v2 é a do hadoop-project 3.5.0 (aws-java-sdk-v2.version). O bundle tem ~690 MB (issue #69).
# Download longo: `-C -` retoma de onde parou e `--speed-limit/--speed-time` abortam conexão parada
# (sem eles, uma queda no meio deixava o curl travado por horas antes do retry).
RUN curl -fSL -C - --retry 10 --retry-all-errors --retry-delay 5 --speed-limit 100000 --speed-time 30 https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar \
         -o /opt/spark/jars/hadoop-aws-3.5.0.jar && \
    curl -fSL -C - --retry 10 --retry-all-errors --retry-delay 5 --speed-limit 100000 --speed-time 30 https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar \
         -o /opt/spark/jars/bundle-2.35.4.jar && \
    curl -fSL -C - --retry 10 --retry-all-errors --retry-delay 5 --speed-limit 100000 --speed-time 30 https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
         -o /opt/spark/jars/postgresql-42.7.13.jar

# Dependência de compile do hadoop-aws 3.5.0 (S3 Analytics Accelerator, usado pelo S3A): sem ela a
# primeira leitura de s3a:// morre com NoClassDefFoundError (achado na #69). Camada separada para
# não baixar o bundle de novo ao mexer nela; versão do hadoop-project 3.5.0.
RUN curl -fSL -C - --retry 10 --retry-all-errors --retry-delay 5 --speed-limit 100000 --speed-time 30 https://repo1.maven.org/maven2/software/amazon/s3/analyticsaccelerator/analyticsaccelerator-s3/1.3.1/analyticsaccelerator-s3-1.3.1.jar \
         -o /opt/spark/jars/analyticsaccelerator-s3-1.3.1.jar

# As bases 3.5.8 e 4.x criam o usuário spark com HOME=/nonexistent (na 3.5.1 era /home/spark): o Ivy do
# `--packages` (conector do Kafka) gravaria em /nonexistent/.ivy2 e o spark-submit falharia (#66).
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && usermod -d /home/spark spark
ENV HOME=/home/spark

# spark-submit da própria distribuição (issue #66). Antes ele vinha, sem querer, do pyspark que o
# `delta-spark` instalava via pip em /usr/local/bin; sem o delta-spark, o binário sumiu do PATH.
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH=/opt/spark/work-dir

USER spark