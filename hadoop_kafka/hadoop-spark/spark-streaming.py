from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

# Kafka에서 메시지 읽기
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "latest") \
    .load()

# 문자열 변환
messages = df.selectExpr("CAST(value AS STRING)").withColumnRenamed("value", "message")

# HDFS에 저장
query = df.writeStream \
    .format("parquet") \
    .option("checkpointLocation", "file:///tmp/spark-checkpoint") \
    .option("path", "file:///tmp/spark-output") \
    .start()

query.awaitTermination()
