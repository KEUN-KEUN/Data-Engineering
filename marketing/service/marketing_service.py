from abc import ABC, abstractmethod

from marketing.controller.request_form.update_request_form import UpdateRequestForm


class MarketingService(ABC):
    @abstractmethod
    def generateVirtualMarketingData(self):
        pass

    @abstractmethod
    def generateVirtualMarketingDataSet(self):
        pass

    @abstractmethod
    def requestAnalysis(self):
        pass

    @abstractmethod
    def requestDataList(self):
        pass

    @abstractmethod
    def readVirtualMarketingData(self, customer_id: int):
        pass

    @abstractmethod
    def updateVirtualMarketingData(self, form: UpdateRequestForm) -> int:
        pass

    @abstractmethod
    def removeVirtualMarketingData(self, customer_id: int) -> int:
        pass
