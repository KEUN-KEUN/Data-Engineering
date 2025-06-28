import asyncio

from aiokafka.errors import TopicAlreadyExistsError
from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from kafka_manage.repository.kafka_manage_repository import KafkaManageRepository


class KafkaManageRepositoryImpl(KafkaManageRepository):
    def __init__(self, bootstrap_servers: str = "localhost:9094"):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer_tasks: dict[str, asyncio.Task] = {}

    def create_topic(self, topic_name: str) -> dict:
        try:
            admin = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
            topic = NewTopic(name=topic_name, num_partitions=1, replication_factor=1)
            admin.create_topics([topic])

            return { "message": f"Topic '{topic_name}' 생성 완료" }
        except TopicAlreadyExistsError:
            return { "message": f"Topic '{topic_name}' 이미 존재" }
        except Exception as e:
            return { "error": str(e) }
