import asyncio

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
