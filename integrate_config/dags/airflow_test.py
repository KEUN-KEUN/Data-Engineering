from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaProducer
import logging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def send_kafka_message():
    logging.info("Sending message to Kafka...")
    try:
        producer = KafkaProducer(bootstrap_servers='kafka:9092')
        future = producer.send('test-topic', b'Hello from Airflow!')
        result = future.get(timeout=10)  # 전송 완료 확인 (동기 방식)
        producer.flush()
        producer.close()
        logging.info(f"Message sent successfully: {result}")
    except Exception as e:
        logging.error(f"Failed to send message: {e}")
        raise

with DAG(
    dag_id='kafka_test_dag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    tags=['kafka']
) as dag:
    task = PythonOperator(
        task_id='produce_kafka_message',
        python_callable=send_kafka_message
    )
