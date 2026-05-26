# Mage.ai with Spark and Java 17
FROM mageai/mageai:latest

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    openjdk-17-jdk-headless \
    curl \
    netcat-openbsd \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for Spark (Java 17 works with Spark 4.0 and 4.1)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download and install Spark (for spark-submit)
ENV SPARK_VERSION=4.1.0
ENV SPARK_HOME=/opt/spark
RUN curl -fsSL "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    | tar -xz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} \
    && chown -R root:root ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Install Python dependencies for Mage pipelines
RUN pip install --no-cache-dir \
    pyspark==4.1.0 \
    pyarrow>=15.0.0 \
    kafka-python>=2.3.0 \
    psycopg2-binary>=2.9.9

# Create directories
RUN mkdir -p /home/src /opt/mage/data /opt/mage/logs

WORKDIR /home/src

USER root
