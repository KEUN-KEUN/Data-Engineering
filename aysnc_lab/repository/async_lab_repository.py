from abc import ABC, abstractmethod


class AsyncLabRepository(ABC):
    @abstractmethod
    async def save_token_to_queue(self, user_token: str) -> None:
        """
        사용자 토큰을 비동기 큐에 등록한다.
        """
        pass

    @abstractmethod
    async def set_user_status(self, user_token: str, status: str) -> None:
        """
        사용자 토큰의 현재 처리 상태를 저장한다.
        예: "processing", "done", "error"
        """
        pass
