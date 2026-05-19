import asyncio
import json
import os
from contextlib import asynccontextmanager
import threading
from dotenv import load_dotenv
from fastapi import FastAPI
import websockets
from my_kafka.kafka import KAFKA_TOPIC, shutdown_event, startup_event, sync_consume_message
from pydantic import BaseModel, Field
from typing import List


# Load environment variables
load_dotenv()
API_KEY = os.getenv("YOUR_API_KEY")
URL = f"wss://ws.finnhub.io?token={API_KEY}"

# Keep track of the background task so we can clean it up safely
background_connect_task = None
background_consume_task = None

class TradeItem(BaseModel):
    t: int = Field(..., description="Timestamp in milliseconds")
    p: float = Field(..., description="Trade price")
    v: float = Field(..., description="Trade volume/quantity")
    s: str = Field(..., description="Trading pair symbol (e.g., BINANCE:BTCUSDT)")

class TradeMessage(BaseModel):
    type: str = Field(..., description="Event type, always 'trade'")
    data: List[TradeItem]

async def connect_finnhub(app: FastAPI):
    """Background task to maintain the Finnhub WebSocket connection."""
    print("Starting Finnhub consumer background task...")
    
    # Subscription payload
    symbols_to_subscribe = ["AAPL", "AMZN", "BINANCE:BTCUSDT", "IC MARKETS:1"]
    
    while True:
        try:
            async with websockets.connect(URL) as ws:
                print("Connected to Finnhub")

                # Bulk subscribe
                for symbol in symbols_to_subscribe:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": symbol}))
                print(f"Subscribed successfully to: {symbols_to_subscribe}")

                # Continuous data listening loop
                while True:
                    message = await ws.recv()
                    data = json.loads(message)
                    if data.get("type") != "trade":
                        continue
                    trade_payload = TradeMessage(**data)
                    message_dict = trade_payload.model_dump()
                    if hasattr(app.state, "producer") and app.state.producer:
                        await app.state.producer.send_and_wait(KAFKA_TOPIC, message_dict)  # Send to Kafka

        except (websockets.ConnectionClosed, ConnectionRefusedError) as e:
            print(f"Connection lost ({e}). Reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            print("Finnhub consumer task stopped cleanly.")
            break
        except Exception as e:
            print(f"Unexpected error: {e}. Retrying in 5 seconds...")
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manages the startup and shutdown lifecycle of the FastAPI application."""
    try:
        global background_connect_task
        stop_event = threading.Event()
        thread_task = None
        # 1. Fires on Startup: Start Finnhub listener in the background
        await startup_event(app)  # Start Kafka producer as well
        background_connect_task = asyncio.create_task(connect_finnhub(app))
        thread_task = asyncio.create_task(asyncio.to_thread(sync_consume_message, stop_event))
    
        # background_consume_task = asyncio.create_task(consume_mesage(app))
        yield  # The app runs and serves HTTP requests here
    except Exception as e:
        print(f"Error during startup: {e}")    
    # 2. Fires on Shutdown: Clean up the task gracefully
    finally:
        if background_connect_task:
            background_connect_task.cancel()
            try:
                await background_connect_task
            except asyncio.CancelledError:
                pass


        stop_event.set()  # Signal the thread consumer to stop
        if thread_task:
            await thread_task 
        await shutdown_event(app)  # Stop Kafka producer as well
        print("Kafka resources cleaned up cleanly.")

# Initialize FastAPI with the lifespan handler
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "running", "message": "Streaming Finnhub data in background"}


@app.post("/send")
async def send_message(payload: TradeMessage):
    message_dict = payload.model_dump()
    # Asynchronously publish message to Kafka cluster
    await app.state.producer.send_and_wait(KAFKA_TOPIC, message_dict)
    return {"status": "Message published successfully", "data": message_dict}
