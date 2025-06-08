from fastapi import APIRouter, Request

from kafka_system.kafka_utility import send_kafka_message
from kafka_system.request_form.kafka_endpoinrt_request_form import KafkaEndpointRequestForm

kafkaController = APIRouter()

@kafkaController.post("/kafka-endpoint")
async def kafka_endpoint(request: Request, kafka_request: KafkaEndpointRequestForm):
    await send_kafka_message(
        producer=request.app.state.kafka_producer,
        topic="test-topic",
        data=kafka_request.dict()
    )
    return {"status": "processing"}
