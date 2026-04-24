import pytest
import asyncio
from httpx import AsyncClient, ASGITransport
from datetime import datetime, timezone
import time
import os

from src.main import app
from src.store import store
from src.consumer import start_consumer, event_queue

@pytest.fixture(scope="module")
def anyio_backend():
    return 'asyncio'

@pytest.fixture(scope="function", autouse=True)
async def setup_db():
    import src.store
    src.store.DB_PATH = "test_aggregator.db"
    
    # Empty queue
    while not event_queue.empty():
        event_queue.get_nowait()
        event_queue.task_done()
        
    if os.path.exists("test_aggregator.db"):
        try:
            os.remove("test_aggregator.db")
        except OSError:
            pass
            
    await store.connect()
    task = asyncio.create_task(start_consumer())
    yield
    task.cancel()
    await store.close()

@pytest.fixture(scope="function")
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

@pytest.mark.anyio
async def test_schema_validation(client):
    invalid_event = {"timestamp": datetime.now(timezone.utc).isoformat(), "source": "test", "payload": {}}
    response = await client.post("/publish", json=invalid_event)
    assert response.status_code == 422
    
    valid_event = {"topic": "sensor", "event_id": "1", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "test", "payload": {"value": 100}}
    response = await client.post("/publish", json=valid_event)
    assert response.status_code == 202

@pytest.mark.anyio
async def test_deduplication(client):
    event1 = {"topic": "dedup", "event_id": "d1", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "test", "payload": {"hello": "world"}}
    response = await client.post("/publish", json=[event1, event1, event1])
    assert response.status_code == 202
    
    await asyncio.sleep(0.5)
    
    res = await client.get("/events?topic=dedup")
    assert res.status_code == 200
    events = res.json().get("events", [])
    assert len(events) == 1
    assert events[0]["event_id"] == "d1"

@pytest.mark.anyio
async def test_stats_consistency(client):
    event2 = {"topic": "stats_test", "event_id": "s1", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "test", "payload": {}}
    await client.post("/publish", json=event2)
    await asyncio.sleep(0.5)
    
    res = await client.get("/stats")
    assert res.status_code == 200
    stats = res.json()
    assert stats["unique_processed"] > 0
    assert stats["received"] >= stats["unique_processed"]
    assert "stats_test" in stats["topics"]

@pytest.mark.anyio
async def test_persistence_restart(client):
    event_p = {"topic": "persist", "event_id": "p1", "timestamp": datetime.now(timezone.utc).isoformat(), "source": "test", "payload": {}}
    await client.post("/publish", json=event_p)
    await asyncio.sleep(0.5)
    
    await store.close()
    await store.connect()
    
    await client.post("/publish", json=event_p)
    await asyncio.sleep(0.5)
    
    res = await client.get("/events?topic=persist")
    events = res.json().get("events", [])
    assert len(events) == 1

@pytest.mark.anyio
async def test_stress(client):
    events = []
    base_ts = datetime.now(timezone.utc).isoformat()
    for i in range(4000):
        events.append({"topic": "stress", "event_id": f"evt_{i}", "timestamp": base_ts, "source": "stress_test", "payload": {"idx": i}})
    for i in range(1000):
        events.append({"topic": "stress", "event_id": f"evt_{i}", "timestamp": base_ts, "source": "stress_test", "payload": {"idx": i}})
        
    start_time = time.time()
    chunk_size = 1000
    for i in range(0, 5000, chunk_size):
        await client.post("/publish", json=events[i:i+chunk_size])
        
    while not event_queue.empty():
        await asyncio.sleep(0.1)
    
    await asyncio.sleep(1.0)
    end_time = time.time()
    assert end_time - start_time < 10.0
    
    res = await client.get("/events?topic=stress")
    assert len(res.json().get("events", [])) == 4000
