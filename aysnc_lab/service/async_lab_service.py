from abc import ABC, abstractmethod


class AsyncLabService(ABC):
    @abstractmethod
    async def enqueue_user_request(self, user_token: str) -> dict:
        """
        사용자 요청을 비동기 큐에 등록하고 처리 상태를 초기화한다.
        """
        pass
