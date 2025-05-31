from abc import ABC, abstractmethod
from typing import List

from system_log.entity.system_log import SystemLog


class SystemLogRepository(ABC):

    @abstractmethod
    def saveAll(self, systemLogList: List[SystemLog]):
        pass
