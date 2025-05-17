FROM ubuntu:22.04

# Install Java, Scala, Spark dependencies
RUN apt-get update && apt-get install -y openjdk-11-jdk curl scala

# Download and extract Spark
RUN curl -L https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz | tar -xz -C /opt

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark-3.1.2-bin-hadoop3.2
ENV PATH=$SPARK_HOME/bin:$PATH

# Create a user
RUN useradd -ms /bin/bash sparkuser
USER sparkuser
WORKDIR /home/sparkuser
