FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y default-jre
USER airflow

RUN pip install --no-cache-dir kafka-python apache-airflow-providers-apache-spark

RUN which airflow && airflow version
