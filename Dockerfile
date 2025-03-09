# Image: docker.io/apache/airflow:2.10.3-python3.12
FROM --platform=linux/amd64 apache/airflow@sha256:a297f7672778ba65d4d95cd1fee0213133ae8d8dd176fe9a697af918f945368f

USER root

RUN apt-get update && apt-get install -y \
    build-essential \   
    libssl-dev \
    libffi-dev \   
    postgresql-client \
    python3-tk \    
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create and Set up directories
RUN mkdir -p /opt/airflow && \
    mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/config && \
    chown -R airflow:root /opt/airflow && \
    chmod -R g+w /opt/airflow && \
    chmod -R 755 /opt/airflow/logs    
    
USER airflow

ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/plugins:/opt/airflow/config:${PYTHONPATH}" 

COPY --chown=airflow:root requirements.txt /opt/airflow/


# Install Airflow with constraints first
RUN pip install --upgrade pip setuptools wheel && \
    pip install "apache-airflow[amazon,postgres,http]==2.10.3" \
        --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.3/constraints-3.12.txt" && \
    # - no-deps will install pandas without installing its dependencies, which is safer if you want to keep other package versions from the constraints file
    pip install --no-deps pandas==2.2.3

# Install additional requirements without constraints
COPY --chown=airflow:root requirements-additional.txt /opt/airflow/
RUN pip install --no-cache-dir -r /opt/airflow/requirements-additional.txt 

# Install specific packages separately
RUN pip install --no-cache-dir s3fs==2024.10.0 && \
    pip install --no-cache-dir freecurrencyapi==0.1.0

# Verify installations
RUN pip list && \
    python -c "import pandas; import numpy; import scipy; import sklearn; print('All imports successful')"

WORKDIR /opt/airflow


ENTRYPOINT ["airflow"]
CMD ["webserver"]