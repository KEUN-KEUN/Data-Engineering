from abc import ABC, abstractmethod


class KafkaManageRepository(ABC):
    @abstractmethod
    def create_topic(self, topic_name: str) -> dict:
        pass
    