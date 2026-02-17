FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu

USER root


RUN apt-get update && apt-get install -y netcat && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/hadoop/etc/hadoop

COPY infra/hadoop-conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY infra/hadoop-conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml

WORKDIR /opt/spark/jobs
COPY clickstream_streaming.py .


ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV USER=spark

USER spark

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8", \
     "--conf", "spark.driver.memory=1g", \
     "--conf", "spark.executor.memory=1g", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "clickstream_streaming.py"]
