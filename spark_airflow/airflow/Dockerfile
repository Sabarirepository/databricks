FROM apache/airflow:2.7.3

USER root

# Install Java and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-11-jre-headless \
    wget \
    procps && \
    apt-get autoremove -yqq --purge && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
RUN export JAVA_HOME

USER airflow

# Install Spark
ENV SPARK_VERSION="3.5.0"
ENV HADOOP_VERSION="3"
ENV SPARK_HOME /usr/local/spark
ENV PATH $PATH:${SPARK_HOME}/bin

RUN cd "/tmp" && \
    wget "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    tar -xzf "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" && \
    mkdir -p "${SPARK_HOME}" && \
    mv "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}"/* "${SPARK_HOME}/" && \
    rm "spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Install PySpark and other Python dependencies
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==2.1.3 \
    pyspark==${SPARK_VERSION}

# Copy any additional requirements or scripts
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Set PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:${AIRFLOW_HOME}"

# Copy DAGs and other necessary files
COPY --chown=airflow:root ./dags /opt/airflow/dags