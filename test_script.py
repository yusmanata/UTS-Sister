import asyncio
import httpx
from datetime import datetime, timezone

async def test_aggregator():
    async with httpx.AsyncClient(base_url="http://127.0.0.1:8000") as client:
        # Publish single event
        ev1 = {
            "topic": "sensor_A",
            "event_id": "1",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "sensor_test",
            "payload": {"temp": 25.5}
        }
        res = await client.post("/publish", json=ev1)
        print("Publish single:", res.status_code, res.json())
        
        # Publish batch including a duplicate
        ev2 = {
            "topic": "sensor_A",
            "event_id": "2",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "source": "sensor_test",
            "payload": {"temp": 26.0}
        }
        ev1_dup = ev1.copy()
        
        res = await client.post("/publish", json=[ev2, ev1_dup])
        print("Publish batch:", res.status_code, res.json())
        
        # Give consumer time to process
        await asyncio.sleep(1)
        
        # Check Events
        res = await client.get("/events?topic=sensor_A")
        print("Events:", res.status_code, [e["event_id"] for e in res.json().get("events", [])])
        
        # Check Stats
        res = await client.get("/stats")
        print("Stats:", res.status_code, res.json())

if __name__ == "__main__":
    asyncio.run(test_aggregator())
