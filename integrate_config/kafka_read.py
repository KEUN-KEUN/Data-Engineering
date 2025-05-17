from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka Consumer 설정
conf = {
    'bootstrap.servers': 'localhost:9092',  # Kafka 서버 주소
    'group.id': 'my_consumer_group',         # 소비자 그룹
    'auto.offset.reset': 'earliest'          # 처음부터 데이터 읽기
}

consumer = Consumer(conf)

# Kafka 토픽 구독
consumer.subscribe(['marketing.analysis.request'])

# 메시지 읽기
try:
    while True:
        msg = consumer.poll(1.0)  # 1초 대기 후 메시지 확인

        if msg is None:
            continue  # 메시지가 없으면 계속 대기
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # EOF 도달 시 로그 출력
                print(f"End of partition reached {msg.topic()} [{msg.partition}] at offset {msg.offset()}")
            else:
                raise KafkaException(msg.error())
        else:
            # 메시지 처리
            print(f"Received message: {msg.value().decode('utf-8')}")
finally:
    # Consumer 종료 시 리소스 해제
    consumer.close()
