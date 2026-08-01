FROM apache/airflow:3.3.0-python3.11

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.0/constraints-3.11.txt"