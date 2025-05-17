from pyspark.sql import SparkSession

# Spark 세션 생성
spark = SparkSession.builder \
    .appName("Kafka to Spark") \
    .getOrCreate()

# Kafka에서 스트리밍 데이터를 읽기
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "marketing.analysis.request") \
    .load()

# Kafka 메시지의 value 필드를 문자열로 변환
kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

# 실시간으로 처리한 데이터를 출력
query = kafka_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 스트리밍 종료까지 대기
query.awaitTermination()
