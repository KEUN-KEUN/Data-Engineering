FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV HADOOP_VERSION=3.3.6
ENV SPARK_VERSION=3.5.1

# 필수 패키지 설치
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk python3 python3-pip wget curl ssh rsync sudo

# PySpark 및 Kafka 설치
RUN pip3 install pyspark kafka-python

# openssh-server 설치 및 설정
RUN apt-get update && \
    apt-get install -y openssh-server sudo && \
    mkdir /var/run/sshd

# Hadoop 설치
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz && \
    mv hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

# Spark 설치 (archive에서 다운로드)
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Kafka 관련 JAR 다운로드
RUN mkdir -p /opt/spark/jars && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.1/snappy-java-1.1.10.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar -P /opt/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar -P /opt/spark/jars/

# ARG와 ENV로 root 비밀번호 전달
ARG ROOT_PASSWORD
ENV ROOT_PASSWORD=${ROOT_PASSWORD}

# root 비밀번호 및 SSH 설정
RUN echo "root:${ROOT_PASSWORD}" | chpasswd && \
    sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config

# hdfs 사용자 생성 및 비밀번호 설정
RUN useradd -m hdfs && \
    echo "hdfs:hdfs" | chpasswd && \
    echo 'hdfs ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# Hadoop/Spark 권한 설정
RUN chown -R hdfs:hdfs /opt/hadoop /opt/spark && \
    chmod -R 755 /opt/hadoop /opt/spark

RUN apt-get install -y vim

# 환경 변수 설정
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$HADOOP_HOME/bin:$JAVA_HOME/bin:$PATH

# Hadoop 설정 복사
COPY hadoop-config/*.xml /opt/hadoop/etc/hadoop/

# 초기화 및 엔트리포인트 스크립트
COPY init-hdfs.sh /app/init-hdfs.sh
RUN chmod +x /app/init-hdfs.sh

COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

# SSH key 없이 password 기반 접속 가능하도록 설정 (hdfs 사용자 홈에)
USER hdfs
RUN mkdir -p /home/hdfs/.ssh && \
    echo "Host localhost\n   StrictHostKeyChecking no\n   UserKnownHostsFile=/dev/null\n   PreferredAuthentications password\n   PubkeyAuthentication no" > /home/hdfs/.ssh/config && \
    chmod 600 /home/hdfs/.ssh/config

# SSH 데몬은 root 권한으로 실행되어야 하므로 entrypoint 내부에서 root로 전환하여 실행
USER root
RUN service ssh start

# Hadoop 데몬은 hdfs 사용자로 실행할 것이므로 USER 전환
USER hdfs

# HDFS 관련 환경 변수
ENV HDFS_NAMENODE_USER=hdfs
ENV HDFS_DATANODE_USER=hdfs
ENV HDFS_SECONDARYNAMENODE_USER=hdfs

# ENTRYPOINT 설정
ENTRYPOINT ["/app/entrypoint.sh"]
