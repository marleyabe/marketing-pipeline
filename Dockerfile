FROM apache/airflow:3.0.0-python3.12

USER root
RUN mkdir -p /opt/pipeline && chown -R airflow:0 /opt/pipeline
USER airflow

COPY requirements.txt /opt/pipeline/requirements.txt
RUN pip install --no-cache-dir -r /opt/pipeline/requirements.txt

COPY src/ /opt/pipeline/src/
COPY sql/ /opt/pipeline/sql/
COPY dags/ /opt/airflow/dags/

ENV PYTHONPATH="/opt/pipeline:${PYTHONPATH}"
