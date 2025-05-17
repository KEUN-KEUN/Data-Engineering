from pyspark.sql import SparkSession
from pyspark.sql.functions import col

print("[INFO] Creating Spark session...")
spark = SparkSession.builder \
    .appName("KafkaRawMessagePrinter") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")
print("[INFO] Spark session created.")

print("[INFO] Reading from Kafka topic...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.219.106:9092") \
    .option("subscribe", "marketing.analysis.request") \
    .option("startingOffsets", "earliest") \
    .load()

raw_kafka_stream = df.selectExpr("CAST(value AS STRING) as raw_value")

print("[INFO] Printing raw Kafka messages to console...")

raw_query = raw_kafka_stream.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

raw_query.awaitTermination()