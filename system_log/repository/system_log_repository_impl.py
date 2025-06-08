from abc import ABC, abstractmethod
from textwrap import dedent
from typing import List

from aiomysql import Pool

from system_log.entity.system_log import SystemLog
from system_log.repository.system_log_repository import SystemLogRepository


class SystemLogRepositoryImpl(SystemLogRepository):

    def __init__(self, db_pool: Pool):
        self.db_pool = db_pool

    async def saveAll(self, logs: List[SystemLog]):
        async with self.db_pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(
                    dedent("""
                        INSERT INTO system_logs (timestamp, user_id, action, duration_ms)
                        VALUES (%s, %s, %s, %s)
                    """).strip(),
                    [
                        (log.timestamp, log.user_id, log.action, log.duration_ms)
                        for log in logs
                    ]
                )
                await conn.commit()
    