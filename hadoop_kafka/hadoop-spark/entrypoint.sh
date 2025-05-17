#!/bin/bash
set -e

echo "🔐 Starting SSH daemon..."
sudo service ssh start || { echo "❌ Failed to start SSH"; exit 1; }

echo "🌀 Initializing HDFS..."
/app/init-hdfs.sh || { echo "❌ Failed to initialize HDFS"; exit 1; }

echo "🚀 Starting Spark Streaming..."
/opt/spark/bin/spark-submit --master local[*] /app/spark-streaming.py || { echo "❌ Spark job failed"; exit 1; }

# DEBUG: 쉘로 들어가서 실행 확인
echo "DEBUG: Entering bash shell"
#bash
