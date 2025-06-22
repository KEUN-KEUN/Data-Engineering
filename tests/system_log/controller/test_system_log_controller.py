import pytest
from httpx import AsyncClient
from fastapi import FastAPI, status, Depends
from unittest.mock import AsyncMock

from system_log.controller.system_log_controller import systemLogRouter, injectSystemLogService
from system_log.service.system_log_service_impl import SystemLogServiceImpl


@pytest.fixture
def app_with_mocked_service():
    mock_service = AsyncMock(spec=SystemLogServiceImpl)
    app = FastAPI()

    # 오버라이드 DI
    app.dependency_overrides[injectSystemLogService] = lambda: mock_service
    app.include_router(systemLogRouter)

    return app, mock_service


@pytest.mark.asyncio
async def test_record_csv(app_with_mocked_service):
    app, mock_service = app_with_mocked_service

    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/system-log/record-csv")

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json()["success"] is True
    mock_service.recordCsv.assert_awaited_once()


@pytest.mark.asyncio
async def test_analysis_csv(app_with_mocked_service):
    app, mock_service = app_with_mocked_service

    mock_service.analysisCsv.return_value = {
        "threshold": 1234,
        "user_avg": [],
        "anomalies": [],
        "hourly_requests": []
    }

    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.post("/system-log/analysis")

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json()["success"] is True
    assert response.json()["data"]["threshold"] == 1234
    mock_service.analysisCsv.assert_awaited_once()
