import aiosqlite
import json
from src.models import EventModel

DB_PATH = "aggregator.db"

class EventStore:
    def __init__(self):
        self.conn = None
        # In-memory stats to reduce DB read overhead for every GET /stats
        self.stats = {
            "received": 0,
            "unique_processed": 0,
            "duplicate_dropped": 0
        }

    async def connect(self):
        self.conn = await aiosqlite.connect(DB_PATH)
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic TEXT,
                event_id TEXT,
                timestamp TEXT,
                source TEXT,
                payload TEXT,
                UNIQUE(topic, event_id)
            )
        ''')
        await self.conn.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value INTEGER
            )
        ''')
        await self.conn.commit()
        await self.load_stats()
        
    async def load_stats(self):
        # Initialize default stats if empty
        for k in self.stats.keys():
            await self.conn.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, 0)", (k,))
        await self.conn.commit()
        
        async with self.conn.execute("SELECT key, value FROM stats") as cursor:
            async for row in cursor:
                if row[0] in self.stats:
                    self.stats[row[0]] = row[1]

    async def update_stat(self, key: str, increment: int = 1):
        self.stats[key] += increment
        await self.conn.execute("UPDATE stats SET value = value + ? WHERE key = ?", (increment, key))
        await self.conn.commit()

    async def is_duplicate(self, topic: str, event_id: str) -> bool:
        async with self.conn.execute("SELECT 1 FROM events WHERE topic = ? AND event_id = ?", (topic, event_id)) as cursor:
            row = await cursor.fetchone()
            return row is not None

    async def save_event(self, event: EventModel) -> bool:
        """Saves the event. Returns True if saved, False if duplicate constraint hit."""
        try:
            await self.conn.execute(
                "INSERT INTO events (topic, event_id, timestamp, source, payload) VALUES (?, ?, ?, ?, ?)",
                (event.topic, event.event_id, event.timestamp.isoformat(), event.source, json.dumps(event.payload))
            )
            await self.conn.commit()
            return True
        except aiosqlite.IntegrityError:
            # Duplicate (topic, event_id)
            return False

    async def get_events_by_topic(self, topic: str) -> list[dict]:
        events = []
        async with self.conn.execute("SELECT event_id, timestamp, source, payload FROM events WHERE topic = ?", (topic,)) as cursor:
            async for row in cursor:
                events.append({
                    "topic": topic,
                    "event_id": row[0],
                    "timestamp": row[1],
                    "source": row[2],
                    "payload": json.loads(row[3])
                })
        return events

    async def get_all_topics(self) -> list[str]:
        topics = []
        async with self.conn.execute("SELECT DISTINCT topic FROM events") as cursor:
            async for row in cursor:
                topics.append(row[0])
        return topics

    async def close(self):
        if self.conn:
            await self.conn.close()

# Singleton instance
store = EventStore()
