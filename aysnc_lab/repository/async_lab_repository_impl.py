from aysnc_lab.async_context import user_token_queue, set_user_status
from aysnc_lab.repository.async_lab_repository import AsyncLabRepository


class AsyncLabRepositoryImpl(AsyncLabRepository):

    async def save_token_to_queue(self, user_token: str) -> None:
        await user_token_queue.put(user_token)

    async def set_user_status(self, user_token: str, status: str) -> None:
        await set_user_status(user_token, status)
