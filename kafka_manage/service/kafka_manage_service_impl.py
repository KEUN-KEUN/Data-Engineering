from kafka_manage.repository.kafka_manage_repository_impl import KafkaManageRepositoryImpl
from kafka_manage.service.kafka_manage_service import KafkaManageService


class KafkaManageServiceImpl(KafkaManageService):
    def __init__(self):
        self.repository = KafkaManageRepositoryImpl()

    def create_topic(self, topic_name: str) -> dict:
        return self.repository.create_topic(topic_name)

    async def publish(self, topic: str, message: dict) -> dict:
        await self.repository.send_message(topic, message)
        return { "status": "메시지 전송 완료" }

    async def subscribe(self, topic: str) -> dict:
        return await self.repository.subscribe(topic)

    async def unsubscribe(self, topic: str) -> dict:
        return await self.repository.unsubscribe(topic)
