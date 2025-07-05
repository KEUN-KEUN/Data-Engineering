import asyncio

from fastapi import APIRouter, Request

from kafka_manage.service.kafka_manage_service_impl import KafkaManageServiceImpl

kafkaManageRouter = APIRouter(prefix="/kafka-manage", tags=["kafka Manage"])
# Kafka를 제어하기 위한 라터우 구성
# prefix를 지정해서 API 경로가 /kafka-manage 로 시작하게 설정

service = KafkaManageServiceImpl()

# 토픽 생성 API 진입점
# @kafkaManageRouter를 통해 POST 요청을 받는 엔드 포인트로서 /kafka-manage/create-topic을 지정
@kafkaManageRouter.post("/create-topic")
def create_topic(request: Request):
    # Request를 통해 외부에서 수신되는 요청을 받음
    # request.json()을 실행하기 위해 asyncio의 이벤트 루프를 강제로 실행 (비동기로 실행됨)
    # body.get("topic_name") 을 통해 JSON 요청 중 topic_name이라는 정보를 추출
    body = asyncio.run(request.json())
    topic_name = body.get("topic_name")

    # 서비스 계층의 create_topic으로 topic_name을 전달
    return service.create_topic(topic_name)

@kafkaManageRouter.post("/publish")
async def publish_message(request: Request):
    body = await request.json()
    topic = body.get("topic")
    message = body.get("message")

    return await service.publish(topic, message)

@kafkaManageRouter.post("/subscribe")
async def subscribe_topic(request: Request):
    body = await request.json()
    topic = body.get("topic")

    return await service.subscribe(topic)

@kafkaManageRouter.post("/unsubscribe")
async def unsubscribe_topic(request: Request):
    body = await request.json()
    topic = body.get("topic")

    return await service.unsubscribe(topic)