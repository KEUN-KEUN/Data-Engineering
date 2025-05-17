#!/bin/bash
pip install --no-cache-dir kafka-python

# DB 초기화 (초기화가 이미 된 경우 문제 없음)
airflow db init

# 명령 인자 그대로 실행
exec "$@"
