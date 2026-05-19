import json
import os
import threading
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "fastapi-logs")
consumer_log_file_path = os.getenv("consumer_log_file_path", "kafka_consumer.log")
producer: AIOKafkaProducer = None

class Message(BaseModel):
    timestamp: str
    symbol: str
    price: float
    volume: int

async def startup_event(app):
    # global producer
    app.state.producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode("utf-8")
                                )
    await app.state.producer.start()
    print("Kafka producer started")

async def shutdown_event(app):
    if hasattr(app.state, "producer") and app.state.producer:
        await app.state.producer.stop()
        print("Kafka producer stopped")
    
    if hasattr(app.state, "consumer") and app.state.consumer:
        await app.state.consumer.stop()
        print("Kafka consumer stopped")

async def consume_mesage(app):
    app.state.consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="fastapi-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    await app.state.consumer.start()
    print("Kafka consumer started")

    log_file_path = consumer_log_file_path
    try:
        print("Starting Kafka consumer loop...")
        async for msg in app.state.consumer:
            print("YES")
            log_entry = f"[Consumer] Received message: {msg.value} on topic {msg.topic}"
            print(log_entry)
            with open(log_file_path, "a", encoding="utf-8") as log_file:
                log_file.write(log_entry + "\n")
                log_file.flush()
    finally:
        await app.state.consumer.stop()

import json
from kafka import KafkaConsumer

def sync_consume_message(stop_event: threading.Event):
    """This runs entirely inside its own isolated OS thread."""
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=["localhost:9092"],
        group_id="fastapi-consumer-group1",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=1000  # Adjust as needed to allow periodic stop checks
    )
    
    print("[Thread-Consumer] Dedicated background thread started.")
    try:

        while not stop_event.is_set():
            for msg in consumer:  # This blocks the thread, but NOT FastAPI

                log_entry = f"[Thread-Consumer] Received: {msg.value}\n"
                
                with open(consumer_log_file_path, "a", encoding="utf-8") as file:
                    file.write(log_entry)
                if stop_event.is_set():
                    break

    except Exception as e:
        print(f"Error in thread consumer: {e}")
    finally:
        consumer.close()
        print("[Thread-Consumer] Kafka consumer closed cleanly.")