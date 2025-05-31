from abc import ABC, abstractmethod


class SystemLogService(ABC):
    @abstractmethod
    def recordCsv(self):
        pass

    @abstractmethod
    def analysisCsv(self) -> dict:
        pass
