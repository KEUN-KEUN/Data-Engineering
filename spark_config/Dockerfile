# Spark master와 worker 컨테이너를 위한 기본 이미지
FROM bitnami/spark:latest

# Spark 작업을 위한 디렉토리 생성
RUN mkdir -p /opt/bitnami/spark/jobs

# 작업 디렉토리 권한 설정 (권한 문제 예방)
RUN chmod -R 777 /opt/bitnami/spark/jobs

# 필요한 Python 스크립트 추가 (여기서는 예시로 'analysis_stream.py' 사용)
COPY ./analysis_stream.py /opt/bitnami/spark/jobs/analysis_stream.py

# 예시: Spark 관련 환경 변수 설정
# ENV SPARK_HOME=/opt/bitnami/spark
# ENV PATH=$SPARK_HOME/bin:$PATH
