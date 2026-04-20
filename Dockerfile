FROM apache/airflow:2.10.3-python3.12

ENV PYTHONPATH=/opt/airflow \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    AIRFLOW__CORE__EXECUTOR=LocalExecutor \
    AIRFLOW__CORE__DAGS_FOLDER=/opt/airflow/dags \
    AIRFLOW__WEBSERVER__EXPOSE_CONFIG=False

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends build-essential curl postgresql-client \
 && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
