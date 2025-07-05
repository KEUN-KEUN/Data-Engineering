from abc import ABC, abstractmethod


class KafkaManageService(ABC):
    @abstractmethod
    def create_topic(self, topic_name: str) -> dict:
        pass

    @abstractmethod
    async def publish(self, topic: str, message: dict) -> dict:
        pass

    @abstractmethod
    async def subscribe(self, topic: str) -> dict:
        pass

    @abstractmethod
    async def unsubscribe(self, topic: str) -> dict:
        pass
