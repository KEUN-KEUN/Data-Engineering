import asyncio

from fastapi import FastAPI

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from async_db.database import getMySqlPool, createTableIfNeccessary
from vector_db.database import getMongoDBPool
from kafka_system.consumer import testTopicConsume

from redis.asyncio import Redis

async def init_mysql(app: FastAPI):
    app.state.dbPool = await getMySqlPool()
    await createTableIfNeccessary(app.state.dbPool)

async def init_redis(app: FastAPI):
    app.state.redis = Redis(host="localhost", port=6379, decode_responses=True)

async def init_vector_db(app: FastAPI):
    app.state.vectorDBPool = await getMongoDBPool()

async def init_kafka(app: FastAPI):
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9094',
        client_id='fastapi-kafka-producer'
    )
    app.state.kafka_consumer = AIOKafkaConsumer(
        'completion_topic',
        bootstrap_servers='localhost:9094',
        group_id="my_group",
        client_id='fastapi-kafka-consumer'
    )
    app.state.kafka_test_topic_consumer = AIOKafkaConsumer(
        'test-topic',
        bootstrap_servers='localhost:9094',
        group_id="another_group",
        client_id='fastapi-kafka-consumer'
    )

    await app.state.kafka_producer.start()
    await app.state.kafka_consumer.start()
    await app.state.kafka_test_topic_consumer.start()

    asyncio.create_task(testTopicConsume(app))

async def shutdown_mysql(app: FastAPI):
    if pool := getattr(app.state, 'dbPool', None):
        pool.close()
        await pool.wait_closed()

async def shutdown_redis(app: FastAPI):
    if redis := getattr(app.state, 'redis', None):
        await redis.close()

async def shutdown_vector_db(app: FastAPI):
    if pool := getattr(app.state, 'vectorDBPool', None):
        pool.close()
        await pool.wait_closed()

async def shutdown_kafka(app: FastAPI):
    for client in ["kafka_producer", "kafka_consumer", "kafka_test_topic_consumer", "kafka_analysis_consumer"]:
        if client_obj := getattr(app.state, client, None):
            await client_obj.stop()

# lifespan 진입점
async def lifespan(app: FastAPI):
    try:
        app.state.stop_event = asyncio.Event()

        await init_mysql(app)
        await init_redis(app)
        await init_vector_db(app)
        await init_kafka(app)

        yield
    finally:
        await shutdown_mysql(app)
        await shutdown_redis(app)
        await shutdown_vector_db(app)
        app.state.stop_event.set()
        await shutdown_kafka(app)
