FROM apache/airflow:2.9.0

USER root

RUN apt-get update && \
    apt-get install -y default-jdk wget && \
    apt-get clean

RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /opt/postgresql-jdbc.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.0/spark-sql-kafka-0-10_2.12-3.5.0.jar \
    -O /opt/spark-sql-kafka.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    -O /opt/kafka-clients.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.0/spark-token-provider-kafka-0-10_2.12-3.5.0.jar \
    -O /opt/spark-token-provider-kafka.jar

RUN wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -O /opt/commons-pool2.jar

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    python-dotenv \
    psycopg2-binary \
    kafka-python