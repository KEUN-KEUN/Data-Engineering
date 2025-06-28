import os
import asyncio
import warnings
import aiomysql

from dotenv import load_dotenv
from fastapi import FastAPI

from aysnc_lab.controller.async_lab_controller import asyncLabRouter
from config.cors_config import CorsConfig
from config.initializer import lifespan
from kafka_manage.controller.kafka_manage_controller import kafkaManageRouter
from kafka_system.kafka_controller import kafkaController
from marketing.controller.marketing_controller import marketingRouter
from system_log.controller.system_log_controller import systemLogRouter
from webflux_thread.controller.webflux_thread_controller import webfluxThreadRouter
from websocket.websocket_controller import websocketController
from kafka_system.topic_manager import create_kafka_topics

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
app.include_router(webfluxThreadRouter)
app.include_router(systemLogRouter)
app.include_router(asyncLabRouter)
app.include_router(kafkaManageRouter)

# 앱 실행
if __name__ == "__main__":
    import uvicorn

    host = os.getenv("APP_HOST")
    port = int(os.getenv("APP_PORT"))

    asyncio.run(create_kafka_topics())
    uvicorn.run(app, host=host, port=port)
