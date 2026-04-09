FROM apache/airflow:2.9.0

USER root

RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean

RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /opt/postgresql-jdbc.jar

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark \
    python-dotenv \
    psycopg2-binary