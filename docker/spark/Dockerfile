FROM apache/spark:3.5.1

USER root

RUN pip3 install --no-cache-dir \
    boto3==1.34.74 \
    s3fs==2024.3.1 \
    pydantic==2.7.1 \
    pydantic-settings==2.2.1 \
    python-dotenv==1.0.1 \
    loguru==0.7.2 \
    delta-spark==3.1.0 \
    psycopg2-binary==2.9.9

RUN curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
         -o /opt/spark/jars/hadoop-aws-3.3.4.jar && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
         -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar && \
    curl -fSL https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar \
         -o /opt/spark/jars/postgresql-42.7.3.jar

RUN mkdir -p /home/spark && chown -R spark:spark /home/spark

ENV PYTHONPATH=/opt/spark/work-dir

USER spark