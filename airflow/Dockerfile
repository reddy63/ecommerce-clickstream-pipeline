FROM apache/airflow:2.9.3

USER root


RUN apt-get update && \
    apt-get install -y openjdk-17-jdk curl wget && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH


RUN wget https://downloads.apache.org/spark/spark-3.5.8/spark-3.5.8-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.8-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.8-bin-hadoop3 /opt/spark && \
    rm spark-3.5.8-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

USER airflow
RUN pip install --no-cache-dir psycopg2-binary
