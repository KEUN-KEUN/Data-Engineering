import asyncio

from aysnc_lab.repository.async_lab_repository_impl import AsyncLabRepositoryImpl
from aysnc_lab.service.async_lab_service import AsyncLabService


class AsyncLabServiceImpl(AsyncLabService):
    def __init__(self):
        self.repository = AsyncLabRepositoryImpl()

    async def enqueue_user_request(self, user_token: str) -> dict:
        # 큐에 넣고 상태 세팅
        await self.repository.save_token_to_queue(user_token)
        await self.repository.set_user_status(user_token, "processing")

        # 작업 비동기로 실행
        asyncio.create_task(self._long_running_task(user_token))

        return {
            "status": "enqueued",
            "user_token": user_token
        }

    async def _long_running_task(self, user_token: str):
        total_seconds = 300
        interval = 60

        for remaining in range(total_seconds, 0, -interval):
            print(f"[{user_token}] Remaining time: {remaining // 60} minutes")
            await asyncio.sleep(interval)

        print(f"[{user_token}] Task complete")
        await self.repository.set_user_status(user_token, "done")
