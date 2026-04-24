import asyncio
import httpx
import os
import uuid
import random
from datetime import datetime, timezone

AGGREGATOR_URL = os.getenv("AGGREGATOR_URL", "http://localhost:8080")

async def simulate_publisher():
    print(f"Publisher starting, target: {AGGREGATOR_URL}")
    async with httpx.AsyncClient() as client:
        while True:
            # Generate a batch of events with 20% guaranteed duplicates inside the batch
            # or intentionally sending the same batch
            
            topic = f"sensor_{random.randint(1, 5)}"
            events = []
            
            # Create 8 unique events
            for _ in range(8):
                events.append({
                    "topic": topic,
                    "event_id": str(uuid.uuid4()),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "source": "simulated_publisher",
                    "payload": {"value": random.random() * 100}
                })
                
            # Create 2 explicit duplicates
            if events:
                dup1 = events[0].copy()
                dup2 = events[1].copy()
                events.append(dup1)
                events.append(dup2)
                
            try:
                res = await client.post(f"{AGGREGATOR_URL}/publish", json=events)
                print(f"[{datetime.now().isoformat()}] Sent 10 events. Response: {res.status_code}")
            except Exception as e:
                print(f"Failed to publish events: {e}")
                
            await asyncio.sleep(2)

if __name__ == "__main__":
    asyncio.run(simulate_publisher())
