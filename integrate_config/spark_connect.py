from pyspark.sql import SparkSession

import socket
local_ip = socket.gethostbyname(socket.gethostname())

# Docker로 실행 중인 Spark Master에 연결
spark = SparkSession.builder \
    .appName("PythonSparkTest") \
    .master("spark://localhost:7077") \
    .config("spark.driver.host", local_ip) \
    .getOrCreate()

# 간단한 작업 수행
df = spark.range(1, 1001)
print("✅ Spark 연결 성공")

# 카운트 실행
count = df.count()
print(f"👉 DataFrame row count: {count}")

# 정리
spark.stop()
