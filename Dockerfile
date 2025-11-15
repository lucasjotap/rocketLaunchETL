FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

USER airflow

# Copy requirements and install Python dependencies
COPY --chown=airflow:root requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy DAGs and data directories
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root data_files/ ${AIRFLOW_HOME}/data_files/

# Create necessary directories
RUN mkdir -p ${AIRFLOW_HOME}/logs ${AIRFLOW_HOME}/data_files

USER airflow

WORKDIR ${AIRFLOW_HOME}

