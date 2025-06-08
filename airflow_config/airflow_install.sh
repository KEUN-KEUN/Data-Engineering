#!/bin/bash

set -e

echo "Conda 환경 활성화"
eval "$(conda shell.bash hook)"
conda activate airflow-env

echo "Loading environment variables from .env"
source .env

echo "Setting version variables..."
export AIRFLOW_VERSION=2.9.1
#export AIRFLOW_VERSION=3.0.1
export PYTHON_VERSION="$(python --version | cut -d ' ' -f 2 | cut -d '.' -f 1,2)"
export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

echo "Installing Airflow ${AIRFLOW_VERSION} with constraints..."
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

echo "Initializing Airflow home..."
export AIRFLOW_HOME=~/sw/airflow
mkdir -p $AIRFLOW_HOME

echo "Initializing Airflow DB..."
airflow db init

echo "👤 Creating admin user..."
airflow users create \
  --username eddi \
  --firstname Sanghoon \
  --lastname Lee \
  --role Admin \
  --email eddi@example.com \
  --password "$AIRFLOW_ADMIN_PASSWORD"

echo "Start webserver: airflow webserver"
echo "Start scheduler: airflow scheduler"