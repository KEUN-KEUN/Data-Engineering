from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType
import socket

# 로컬 IP 설정 (드라이버 호스트용)
local_ip = socket.gethostbyname(socket.gethostname())

# Kafka 메시지의 JSON 스키마 정의
schema = StructType([
    StructField("analysis_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("data", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
        ])
    ), True),
])

# Spark 세션 생성 (로컬에서만 실행되도록 설정)
spark = SparkSession.builder \
    .appName("KafkaAnalysisStream") \
    .master("local[*]") \
    .config("spark.driver.host", local_ip) \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_kafka_checkpoint") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.3") \
    .getOrCreate()

# 로그 레벨 설정
spark.sparkContext.setLogLevel("WARN")

# Kafka 연결 로그 출력
print("📡 Connecting to Kafka at localhost:9092 on topic ANALYSIS_REQUEST_TOPIC")
print(f"📑 Using schema: {schema.simpleString()}")

# Kafka 메시지 수신 (Kafka 접근은 무조건 localhost로 제한)
df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "marketing.analysis.request") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()


# Kafka value → 문자열 → JSON 파싱
parsed = df.selectExpr("CAST(value AS STRING) AS value_str") \
    .withColumn("json", from_json(col("value_str"), schema)) \
    .withColumn("parse_failed", col("json").isNull()) \
    .select("value_str", "json.*", "parse_failed")

# 배치 처리 로직 정의
def process_batch(batch_df, batch_id):
    print(f"\n📦 New Batch Received! ID: {batch_id} | Total records: {batch_df.count()}")

    try:
        # 파싱에 실패한 메시지 처리
        failed = batch_df.filter(col("parse_failed") == True)
        if failed.count() > 0:
            print("❌ Failed to parse the following messages:")
            failed.select("value_str").show(truncate=False)

        # 정상적으로 파싱된 메시지 처리
        valid = batch_df.filter(col("parse_failed") == False).drop("parse_failed", "value_str")
        if valid.count() > 0:
            print("✅ Successfully parsed messages:")
            valid.show(truncate=False)

    except Exception as e:
        print(f"🔥 Error while processing batch {batch_id}: {e}")

# 스트리밍 쿼리 실행
query = parsed.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

print("⏳ Waiting for streaming data...")
query.awaitTermination()
