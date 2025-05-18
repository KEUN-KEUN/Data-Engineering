from abc import ABC, abstractmethod
from typing import List, Optional

from marketing.controller.request_form.update_request_form import UpdateRequestForm
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

    @abstractmethod
    def findById(self, id: int) -> Optional[MarketingData]:
        pass

    @abstractmethod
    def update(self, data: MarketingData) -> int:
        pass

    @abstractmethod
    def deleteById(self, customer_id: int) -> int:
        pass
