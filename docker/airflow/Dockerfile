FROM apache/airflow:2.9.3-python3.11

COPY docker/airflow/requirements.txt /tmp/airflow-local-requirements.txt

RUN pip install --no-cache-dir -r /tmp/airflow-local-requirements.txt
