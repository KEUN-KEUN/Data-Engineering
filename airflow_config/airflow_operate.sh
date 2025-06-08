#!/bin/bash

# sudo apt install postgresql-client -y
# 환경변수 파일 불러오기
source .env

# PostgreSQL 체크
#pg_isready -h localhost -p 5432 -U airflow
pg_isready -h 127.0.0.1 -p 5432 -U airflow
if [ $? -ne 0 ]; then
    echo "❌ PostgreSQL 서버가 실행되고 있지 않습니다. 먼저 실행하세요."
    exit 1
fi

# sudo apt install redis-tools -y
# pip install celery
# Redis 체크
redis-cli -h localhost -p 6379 -a eddi@123 ping | grep -q PONG
if [ $? -ne 0 ]; then
    echo "❌ Redis 서버가 실행되고 있지 않습니다. 먼저 실행하세요."
    exit 1
fi

# Airflow 시작
airflow webserver -p 8082 -D
airflow scheduler -D
celery -A airflow.executors.celery_executor.app worker --loglevel=INFO -D
# celery --broker=${AIRFLOW__CELERY__BROKER_URL} flower --port=5555 -D