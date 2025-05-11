import os
import asyncio
import warnings
import aiomysql

from dotenv import load_dotenv
from fastapi import FastAPI
from config.cors_config import CorsConfig
from config.initializer import lifespan
from kafka.kafka_controller import kafkaController
from marketing.controller.marketing_controller import marketingRouter
from websocket.websocket_controller import websocketController
from kafka.topic_manager import create_kafka_topics

# 초기 설정
warnings.filterwarnings("ignore", category=aiomysql.Warning)
load_dotenv()

# FastAPI 앱 초기화
app = FastAPI(lifespan=lifespan)

# CORS 설정
CorsConfig.middlewareConfig(app)

# 웹소켓 연결 상태 저장소
app.state.connections = set()

# 라우터 등록
app.include_router(kafkaController)
app.include_router(websocketController)
app.include_router(marketingRouter)

# 앱 실행
if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST")
    port = int(os.getenv("APP_PORT"))

    asyncio.run(create_kafka_topics())
    uvicorn.run(app, host=host, port=port)
