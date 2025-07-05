import asyncio
import json

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import TopicAlreadyExistsError
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from kafka_manage.repository.kafka_manage_repository import KafkaManageRepository


class KafkaManageRepositoryImpl(KafkaManageRepository):
    def __init__(self, bootstrap_servers: str = "localhost:9094"):
        self.bootstrap_servers = bootstrap_servers
        # 비동기용 Kafka Producer
        self.producer = None
        # Consumer 관리용 맵
        self.consumer_tasks: dict[str, asyncio.Task] = {}
        # 위 구조가 publish, sbuscribe와 함께 구동할 것을 예측해 볼 수 있습니다.

    def create_topic(self, topic_name: str) -> dict:
        try:
            # KafkaAdminClient는 Kafka 서버에 관리 작업을 수행할 수 있게 해줍니다.
            # 앞서 우리가 docker-compose를 통해 외부에 노출시킨 서비스가 localhost:9094 입니다.
            # 그러므로 localhost:9094로 boostrap 서버를 구성하여
            # Kafka를 관리할 수 있는 Admin을 구성했다 정리하면 됩니다.
            admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            # Kafka에 생성할 토픽을 정의하는 파트
            # 파티션 1개, 레플리케이션 1개 (단일 브로커) 환경으로 구성됩니다.
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            # 위 조건으로 토픽 생성
            admin.create_topics([topic])

            return { "message": f"Topic '{topic_name}' 생성 완료" }
        # 이미 존재하는 토픽을 만들려고 할 때 발생
        except TopicAlreadyExistsError:
            return { "message": f"Topic '{topic_name}' 이미 존재" }
        # 기타 뭔지 모를 오류가 발생하는 상황
        except Exception as e:
            return { "error": str(e) }

    async def send_message(self, topic: str, message: dict) -> None:
        if self.producer is None:
            # AIOKafkaProducer의 경우 kafka에 연결하기 위한 비동기 프로듀서 객체를 생성합니다.
            # kafka 브로커로 앞서 구성한 localhost:9094를 선택합니다.
            # 이후 만들어진 producer를 start()를 통해 실행합니다.
            self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
            await self.producer.start()

        # 특정 토픽에 메시지를 전송합니다.
        # 메시지를 해당 토픽의 적절한 파티션에 할당
        # 브로커에게 메시지를 전송
        await self.producer.send_and_wait(topic, json.dumps(message).encode('utf-8'))

    async def subscribe(self, topic: str) -> dict:
        # Kafka 토픽에 대한 소비 작업을 시작 할 때
        # 이미 해당 토픽에 대해 소비 중이라면 중복으로 구독하는 것을 방지해야 합니다.
        if topic in self.consumer_tasks:
            return { "message": f"이미 '{topic}' 을 구독하고 있습니다." }

        # 비동기 태스크를 구성함.
        # 비동기 태스크에서 __consume_loop() 가 구동됨.
        # 실제로 __consume_loop는 타 언어로 치면 private consume_loop와 같은 것임.
        # 위와 같이 구성함으로서 실제 백그라운드에서 지속적으로 메시지를 구독할 수 있음.
        # 생성한 Task를 구독자 리스트로 관리하여 추후 상태를 관리하기 위한 목적으로 배치하였음.
        task = asyncio.create_task(self.__consume_loop(topic))
        self.consumer_tasks[topic] = task

        return { "message": f"'{topic}' 컨슈머 구동" }

    async def __consume_loop(self, topic: str):
        # Kafka 토픽으로부터 메시지를 지속적으로 수신하는 루프
        # KafkaConsumer의 경우엔 소비할 Kafka Topic을 지정해야 합니다.
        # 또한 Kafka 브로커 주소가 필요하고
        # 같은 그룹 ID를 가진 여러 구독자들이 파티션을 분산 처리 할 수 있도록 group_id 또한 지정해줍니다.
        # auto_offset_reset='eraliest' 의 경우 처음 실행 시 가장 처음 메시지부터 읽을 수 있도록 만듭니다.
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=f"group-{topic}",
            auto_offset_reset="earliest")

        await consumer.start()

        try:
            async for message in consumer:
                print(f"[Kafka][{topic}] {message.value.decode('utf-8')}")
        except Exception as e:
            print(f"[Kafka][{topic}] {e}")
        finally:
            await consumer.stop()

    async def unsubscribe(self, topic: str) -> dict:
        # 현재 topic을 구독 중인지 확인
        task = self.consumer_tasks.get(topic)
        if not task:
            return { "message": f"현재 구독중인 topic({topic})이 아닙니다." }

        # 보다 복잡한 연산인 경우엔 여전히 작업이 진행중일 수 있음
        # task.done() 을 통해서 task가 완료 되었는지 체크합니다.
        # 만약 아직 종료되지 않았다면 task.cancel()을 통해 작업 취소를 진행합니다.
        # cancel()이 즉시 중지하지는 않으며 CancelledError Exception을 유발합니다.
        # 만약 정상적으로 task가 취소되면 그냥 다음으로 진행
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # 앞서 등록한 구독 태스크를 정리합니다.
        del self.consumer_tasks[topic]
        return { "message": f"topic({topic}) 구독 해제가 완료되었습니다." }
