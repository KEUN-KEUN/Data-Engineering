# --- utils/kafka_utils.py ---
import json
from aiokafka import AIOKafkaProducer

async def send_kafka_message(producer: AIOKafkaProducer, topic: str, data: dict):
    await producer.send_and_wait(topic, json.dumps(data).encode())
