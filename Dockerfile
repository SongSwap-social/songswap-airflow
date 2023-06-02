FROM apache/airflow:2.6.1
COPY requirements.txt /
RUN pip install --no-cache-dir "apache-airflow[postgres]==${AIRFLOW_VERSION}" -r /requirements.txt

