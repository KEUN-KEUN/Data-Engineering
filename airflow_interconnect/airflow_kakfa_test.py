import logging
from datetime import datetime

from airflow import DAG
# airflow에서 DAG 객체를 참조합니다.
# DAG는 Directed Acyclic Graph 의 약자로 작업 순서가 정해져 있는 그래프 자료구조를 의미합니다.
# 이 객체를 구성해서 어떤 작업을 언제 어떻게 실행할 것인지 정의할 수 있습니다.
from airflow.operators.python import PythonOperator
# Python 함수를 실행하는 Operator 입니다.
# 우리가 구성한 send_kafka_message를 Airflow에서 구동하도록 지원합니다.
from kafka import KafkaProducer

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
}
# DAG에 공통으로 들어가는 기본 인자들을 구성
# owner <- DAG 소유자를 의미하는데 특별한 의미는 없음.
# start_date <- DAG가 언제부터 시작할 것인지 기준이 됨.
# retries <- 만약 통신 불량이나 기타 문제로 처리가 안 될 경우 몇 번 재시도를 할 것인지 정함.

def send_kafka_message():
    logging.info("Kafka로 메시지 쏘기")
    try:
        # 실제 Kafka가 구동하고 있는 서비스 주소이며
        # 여기의 'test-topic' 으로 'Message from Airflow' 를 전송
        kafka_producer = KafkaProducer(bootstrap_servers='localhost:9094')
        future = kafka_producer.send('test-topic', b'Message from Airflow')
        # 보편적으로 비동기 함수들은 전부 Future 타입을 리턴합니다.
        # 왜냐하면 미래 언젠가 될 것이기 때문인데
        # 아래와 같이 get으로 timeout을 걸면 10초내로 응답이 없으면 실패 처리하게 됩니다.
        result = future.get(timeout=10)
        kafka_producer.flush()
        kafka_producer.close()
        logging.info(f"메시지 전송 성공: {result}")

    except Exception as e:
        logging.error(f"구동 실패 메시:지 {e}")
        raise

# dag_id는 DAG 내에서 사용하는 고유한 id 입니다.
# 실제로 Airflow Webserver에서도 이 dag_id가 UI에 나타납니다.
# 기본 인자는 위에서 지정한 default_args 값들을 배치
# schedule_interval <- @once와 @daily
# @once의 경우 DAG가 한 번만 실행하도록 지정 (서버를 키면 무조건 1번 실행함)
# @daily는 매일 자정에 실행하게 됨
# catchup의 경우 DAG를 처음 등록할 때 과거 날짜에 대해 자동 실행하지 않게 만듭니다
# is_paused_upon_create을 False로 지정함으로서
# DAG가 등록되자마자 바로 실행 가능한 상태로 만들어집니다.
with DAG(
    dag_id='airflow_kakfa_test',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False,
    is_paused_upon_creation=False,
    tags=['airflow', 'kafka']
) as dag:
    # Task의 id 를 produce_kafka_message 로 등록
    # 이 녀석이 실행할 함수는 send_kafka_message가 됩니다.
    # Airflow가 이 Task를 구동할 때 자동 호출됩니다.
    task = PythonOperator(
        task_id='produce_kafka_message',
        python_callable=send_kafka_message
    )