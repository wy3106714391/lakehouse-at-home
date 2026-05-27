# Apache DolphinScheduler with Spark support
FROM apache/dolphinscheduler:3.2.2

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    openjdk-17-jre-headless \
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
    && chown -R dolphinscheduler:dolphinscheduler ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER dolphinscheduler

# Create directories
RUN mkdir -p /opt/dolphinscheduler/tasks /opt/dolphinscheduler/logs /opt/dolphinscheduler/config

WORKDIR /opt/dolphinscheduler
