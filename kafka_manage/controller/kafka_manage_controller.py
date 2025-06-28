import asyncio

from fastapi import APIRouter, Request

from kafka_manage.service.kafka_manage_service_impl import KafkaManageServiceImpl

kafkaManageRouter = APIRouter(prefix="/kafka-manage", tags=["kafka Manage"])

service = KafkaManageServiceImpl()

@kafkaManageRouter.post("/create-topic")
def create_topic(request: Request):
    body = asyncio.run(request.json())
    topic_name = body.get("topic_name")

    return service.create_topic(topic_name)