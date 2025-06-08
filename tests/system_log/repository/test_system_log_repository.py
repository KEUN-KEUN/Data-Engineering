from textwrap import dedent

import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, timezone

from system_log.entity.system_log import SystemLog
from system_log.repository.system_log_repository_impl import SystemLogRepositoryImpl


@pytest.mark.asyncio
async def test_save_all_should_insert_logs():
    logs = [
        SystemLog(
            timestamp=datetime.now(timezone.utc),
            user_id="user123",
            action="create",
            duration_ms=100
        )
    ]

    mock_cursor = AsyncMock()
    mock_cursor.executemany = AsyncMock()

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__aenter__.return_value = mock_cursor
    mock_conn.commit = AsyncMock()

    mock_pool = MagicMock()
    mock_acquire_cm = MagicMock()
    mock_acquire_cm.__aenter__.return_value = mock_conn
    mock_pool.acquire.return_value = mock_acquire_cm

    repo = SystemLogRepositoryImpl(db_pool=mock_pool)

    await repo.saveAll(logs)

    expected_sql = dedent("""
        INSERT INTO system_logs (timestamp, user_id, action, duration_ms)
        VALUES (%s, %s, %s, %s)
    """).strip()

    mock_cursor.executemany.assert_awaited_once_with(
        expected_sql,
        [(log.timestamp, log.user_id, log.action, log.duration_ms) for log in logs]
    )
