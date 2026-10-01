FROM apache/spark:3.5.8

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

RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.797/aws-java-sdk-bundle-1.12.797.jar \
         -o /opt/spark/jars/aws-java-sdk-bundle-1.12.797.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.13/postgresql-42.7.13.jar \
         -o /opt/spark/jars/postgresql-42.7.13.jar

# A base 3.5.8 cria o usuário spark com HOME=/nonexistent (na 3.5.1 era /home/spark): o Ivy do
# `--packages` (conector do Kafka) gravaria em /nonexistent/.ivy2 e o spark-submit falharia (#66).
RUN mkdir -p /home/spark && chown -R spark:spark /home/spark && usermod -d /home/spark spark
ENV HOME=/home/spark

# spark-submit da própria distribuição (issue #66). Antes ele vinha, sem querer, do pyspark que o
# `delta-spark` instalava via pip em /usr/local/bin; sem o delta-spark, o binário sumiu do PATH.
ENV PATH="/opt/spark/bin:${PATH}"
ENV PYTHONPATH=/opt/spark/work-dir

USER spark