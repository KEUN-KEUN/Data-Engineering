from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HDFSToKafka") \
    .getOrCreate()

# HDFS에 저장된 Parquet 데이터 읽기
df = spark.read.parquet("hdfs://localhost:9000/output/messages")

# Kafka로 전송
df.selectExpr("CAST(message AS STRING) AS value") \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("topic", "hdfs-to-kafka-topic") \
  .save()
