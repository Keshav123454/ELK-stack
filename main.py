import asyncio
import json
import os
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI
import websockets

# Load environment variables
load_dotenv()
API_KEY = os.getenv("YOUR_API_KEY")
URL = f"wss://ws.finnhub.io?token={API_KEY}"

# Keep track of the background task so we can clean it up safely
background_connect_task = None

async def connect_finnhub():
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
                    print(data)  # Later, you will likely push this to ELK/Logstash

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
    global background_connect_task
    # 1. Fires on Startup: Start Finnhub listener in the background
    background_connect_task = asyncio.create_task(connect_finnhub())
    
    yield  # The app runs and serves HTTP requests here
    
    # 2. Fires on Shutdown: Clean up the task gracefully
    if background_connect_task:
        background_connect_task.cancel()
        try:
            await background_connect_task
        except asyncio.CancelledError:
            pass

# Initialize FastAPI with the lifespan handler
app = FastAPI(lifespan=lifespan)

@app.get("/")
async def root():
    return {"status": "running", "message": "Streaming Finnhub data in background"}
