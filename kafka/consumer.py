import asyncio
import json
from fastapi import FastAPI

async def testTopicConsume(app: FastAPI):
    consumer = app.state.kafka_test_topic_consumer

    while not app.state.stop_event.is_set():
        try:
            msg = await consumer.getone()
            print(f"Kafka message: {msg}")
            data = json.loads(msg.value.decode("utf-8"))
            print(f"Decoded data: {data}")

            # 여기서 비즈니스 로직 처리
            await asyncio.sleep(60)

            for connection in app.state.connections:
                await connection.send_json({
                    'message': 'Processing completed.',
                    'data': data,
                    'title': "Kafka Test"
                })

        except asyncio.CancelledError:
            print("소비자 태스크 종료")
            break
        except Exception as e:
            print(f"소비 중 에러 발생: {e}")
