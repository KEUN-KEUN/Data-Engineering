import os
from typing import List

import numpy as np
import pandas as pd

from system_log.entity.system_log import SystemLog
from system_log.repository.system_log_repository_impl import SystemLogRepositoryImpl
from system_log.service.system_log_service import SystemLogService
from aiomysql import Pool

class SystemLogServiceImpl(SystemLogService):
    def __init__(self, db_pool: Pool):
        self.systemLogRepository = SystemLogRepositoryImpl(db_pool)

    async def recordCsv(self):
        currentDirectory = os.getcwd()
        print(f"Current directory: {currentDirectory}")

        csvPath = os.path.join(currentDirectory, "resource", "system_log.csv")

        df = pd.read_csv(csvPath, parse_dates=["timestamp"])

        systemLogList: List[SystemLog] = [
            SystemLog(
                timestamp=row['timestamp'],
                user_id=row['user_id'],
                action=row['action'],
                duration_ms=int(row['duration_ms'])
            )
            for _, row in df.iterrows()
        ]

        await self.systemLogRepository.saveAll(systemLogList)

    async def analysisCsv(self) -> dict:
        try:
            currentDirectory = os.getcwd()
            print(f"Current directory: {currentDirectory}")

            csvPath = os.path.join(currentDirectory, "resource", "system_log.csv")

            try:
                df = pd.read_csv(csvPath, parse_dates=['timestamp'])
                print(f"✅ Loaded CSV with {len(df)} rows.")
            except FileNotFoundError:
                print("⚠️ CSV not found. Generating sample data.")
                np.random.seed(42)
                timestamps = pd.date_range("2025-05-01", periods=100, freq="15min")
                users = [f"user_{i:02d}" for i in range(1, 6)]
                actions = ['login', 'query', 'logout']
                data = {
                    'timestamp': np.random.choice(timestamps, size=200),
                    'user_id': np.random.choice(users, size=200),
                    'action': np.random.choice(actions, size=200),
                    'duration_ms': np.random.exponential(scale=1000, size=200).astype(int)
                }
                df = pd.DataFrame(data)

            # 이상값 탐지 (95% 초과)
            threshold = df['duration_ms'].quantile(0.95)
            df['is_anomaly'] = df['duration_ms'] > threshold
            print(f"📊 Anomaly threshold: {threshold}")
            print(f"🚨 Anomalies found: {df['is_anomaly'].sum()}")

            # 사용자별 평균 duration
            user_avg = df.groupby('user_id')['duration_ms'].mean().sort_values()
            print("📈 User avg durations:")
            print(user_avg)

            # 이상치 목록
            anomalies = df[df['is_anomaly']][['timestamp', 'user_id', 'action', 'duration_ms']]
            anomalies['timestamp'] = anomalies['timestamp'].astype(str)

            # 시간대별 요청 수 계산
            df['hour'] = df['timestamp'].dt.hour
            hourly_counts = df.groupby('hour').size().reset_index(name='count')
            hourly_requests = hourly_counts.to_dict(orient='records')

            # 최종 JSON 응답
            return {
                "threshold": threshold,
                "user_avg": user_avg.reset_index().to_dict(orient="records"),
                "anomalies": anomalies.reset_index(drop=True).to_dict(orient="records"),
                "hourly_requests": hourly_requests
            }

        except Exception as e:
            print(f"❌ Error in analysisCsv(): {str(e)}")
            raise
