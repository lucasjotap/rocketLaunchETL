FROM apache/airflow:2.7.3-python3.11

USER root

# Install essential system dependencies (Java required for PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-11-jdk-headless \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Spark and Delta Lake environment variables
ENV SPARK_HOME=/opt/spark
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

USER airflow

# Copy requirements and install Python dependencies
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy Lakehouse Engine packages
COPY --chown=airflow:root lakehouse_engine/ ${AIRFLOW_HOME}/lakehouse_engine/
COPY --chown=airflow:root lakehouse_engine_usage/ ${AIRFLOW_HOME}/lakehouse_engine_usage/

# Copy project files
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root data_files/ ${AIRFLOW_HOME}/data_files/
COPY --chown=airflow:root lakehouse/ ${AIRFLOW_HOME}/lakehouse/
COPY --chown=airflow:root job/ ${AIRFLOW_HOME}/job/

# Copy Docker helper scripts
COPY --chown=airflow:root docker/entrypoint_lakehouse.sh /entrypoint_lakehouse.sh
RUN chmod +x /entrypoint_lakehouse.sh

# Create necessary directories for lakehouse architecture
RUN mkdir -p ${AIRFLOW_HOME}/lakehouse/{bronze,silver,gold}/{finance,economics,currency,blockchain,bitcoin} \
    && mkdir -p ${AIRFLOW_HOME}/logs \
    && mkdir -p ${AIRFLOW_HOME}/lakehouse/schemas/{bronze,silver,gold}

# Set Python path to include project root
ENV PYTHONPATH=${AIRFLOW_HOME}:${PYTHONPATH}

USER airflow
WORKDIR ${AIRFLOW_HOME}
