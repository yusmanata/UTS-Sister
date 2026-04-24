import asyncio
import time
from typing import List, Union
from fastapi import FastAPI, HTTPException, BackgroundTasks, status
from pydantic import ValidationError

from src.models import EventModel, StatsResponse
from src.store import store
from src.consumer import start_consumer, event_queue

# To calculate uptime
start_time = time.time()

async def lifespan(app: FastAPI):
    # Startup
    await store.connect()
    app.state.consumer_task = asyncio.create_task(start_consumer())
    yield
    # Shutdown
    app.state.consumer_task.cancel()
    await store.close()

app = FastAPI(title="Event Aggregator API", lifespan=lifespan)

@app.post("/publish", status_code=status.HTTP_202_ACCEPTED)
async def publish_events(events: Union[EventModel, List[EventModel]]):
    if isinstance(events, EventModel):
        events = [events]
    
    for event in events:
        await event_queue.put(event)
    
    return {"message": f"Accepted {len(events)} event(s) for processing"}

@app.get("/events")
async def get_events(topic: str):
    if not topic:
        raise HTTPException(status_code=400, detail="Query parameter 'topic' is required")
    
    events = await store.get_events_by_topic(topic)
    return {"topic": topic, "events": events}

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    topics = await store.get_all_topics()
    uptime_seconds = time.time() - start_time
    
    # Store.stats was updated in-memory directly for quick access
    stats = store.stats
    
    return StatsResponse(
        received=stats["received"],
        unique_processed=stats["unique_processed"],
        duplicate_dropped=stats["duplicate_dropped"],
        topics=topics,
        uptime_seconds=uptime_seconds
    )
