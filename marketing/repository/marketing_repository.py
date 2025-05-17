from abc import ABC, abstractmethod
from typing import List
from marketing.entity.marketing_data import MarketingData


class MarketingRepository(ABC):

    @abstractmethod
    def create(self, data: MarketingData) -> None:
        pass

    @abstractmethod
    def bulkCreate(self, data: List[MarketingData]) -> None:
        pass

    @abstractmethod
    def findAll(self) -> List[MarketingData]:
        pass
