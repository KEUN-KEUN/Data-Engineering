import asyncio

# 비동기 큐 (스레드 락 없이 동시성 안전)
user_token_queue = asyncio.Queue()

# 상태를 저장할 딕셔너리와 이를 안전하게 다룰 수 있는 큐 기반 명령 처리 방식
user_status = {}

async def set_user_status(user_token: str, status: str):
    user_status[user_token] = status

async def get_user_status(user_token: str) -> str:
    return user_status.get(user_token, "unknown")
