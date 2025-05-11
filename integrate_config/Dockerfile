FROM bitnami/spark:latest

# 루트 사용자로 전환
USER root

# 필요한 디렉터리 생성
RUN mkdir -p /var/lib/apt/lists/partial

# apt-get 업데이트 및 curl 설치
RUN apt-get update && apt-get install -y curl

# 작업 디렉토리 생성 및 권한 부여
RUN mkdir -p /opt/bitnami/spark/jobs
RUN chmod -R 777 /opt/bitnami/spark/jobs

# Spark 의존성 JAR 파일을 자동으로 다운로드하는 스크립트 추가
RUN curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.1.2/spark-sql-kafka-0-10_2.12-3.1.2.jar -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.12-3.1.2.jar
RUN curl -sL https://repo1.maven.org/maven2/org/apache/spark/spark-streaming-kafka-0-10_2.12/3.1.2/spark-streaming-kafka-0-10_2.12-3.1.2.jar -o /opt/bitnami/spark/jars/spark-streaming-kafka-0-10_2.12-3.1.2.jar

# 분석 스트림 스크립트 복사
COPY ./analysis_stream.py /opt/bitnami/spark/jobs/analysis_stream.py
