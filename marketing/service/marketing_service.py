from abc import ABC, abstractmethod

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
