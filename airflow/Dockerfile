FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y gcc libpq-dev python3-dev openjdk-17-jre-headless curl procps

# Install Apache Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME \
    && ln -s $SPARK_HOME/bin/spark-submit /usr/local/bin/spark-submit
ENV PATH="$PATH:$SPARK_HOME/bin"

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
