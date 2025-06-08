import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from system_log.service.system_log_service_impl import SystemLogServiceImpl
from system_log.entity.system_log import SystemLog
import pandas as pd
from datetime import datetime

@pytest.fixture
def mock_repo():
    repo = MagicMock()
    repo.saveAll = AsyncMock()
    return repo

@pytest.fixture
def service(mock_repo):
    service = SystemLogServiceImpl(db_pool=None)
    service.systemLogRepository = mock_repo
    return service

@patch("os.getcwd")
@patch("pandas.read_csv")
@pytest.mark.asyncio
async def test_record_csv(mock_read_csv, mock_getcwd, service, mock_repo):
    # mock working dir
    mock_getcwd.return_value = "/fake/dir"

    # mock DataFrame
    mock_df = pd.DataFrame([{
        'timestamp': pd.Timestamp("2025-05-01 12:00:00"),
        'user_id': 'user_01',
        'action': 'login',
        'duration_ms': 1234
    }])
    mock_read_csv.return_value = mock_df

    await service.recordCsv()

    # Repository의 saveAll이 정확히 호출되었는지 검증
    mock_repo.saveAll.assert_called_once()
    args = mock_repo.saveAll.call_args[0][0]
    assert isinstance(args[0], SystemLog)
    assert args[0].user_id == 'user_01'
    assert args[0].duration_ms == 1234

@patch("os.getcwd")
@patch("pandas.read_csv")
@pytest.mark.asyncio
async def test_analysis_csv(mock_read_csv, mock_getcwd, service):
    mock_getcwd.return_value = "/fake/dir"

    df = pd.DataFrame({
        'timestamp': pd.date_range("2025-05-01", periods=10, freq="1h"),
        'user_id': ['user_01'] * 10,
        'action': ['query'] * 10,
        'duration_ms': [100, 200, 150, 3000, 120, 1000, 110, 5000, 130, 100]
    })

    mock_read_csv.return_value = df

    result = await service.analysisCsv()

    assert 'threshold' in result
    assert 'user_avg' in result
    assert 'anomalies' in result
    assert 'hourly_requests' in result

    assert isinstance(result["threshold"], (int, float))
    assert isinstance(result["user_avg"], list)
    assert isinstance(result["anomalies"], list)
    assert isinstance(result["hourly_requests"], list)
