import asyncio
import logging
from src.models import EventModel
from src.store import store

logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

event_queue: asyncio.Queue[EventModel] = asyncio.Queue()

async def process_event(event: EventModel):
    # Dedup is handled by attempting to save, and catching IntegrityError via save_event
    # Or explicitly checking. Let's do explicit check or just rely on SQLite constraint.
    # Relying on SQLite constraint might be better because it guarantees ACID for concurrent tasks
    # However, since we process events sequentially in this consumer, we can just use store.save_event directly
    
    await store.update_stat("received", 1)
    
    # Memeriksa apakah event merupakan duplikat
    is_saved = await store.save_event(event)
    
    if is_saved:
        await store.update_stat("unique_processed", 1)
        logger.info(f"Processed new event: topic={event.topic}, event_id={event.event_id}")
    else:
        await store.update_stat("duplicate_dropped", 1)
        logger.warning(f"Duplicate event dropped: topic={event.topic}, event_id={event.event_id}")

async def start_consumer():
    logger.info("Starting consumer background worker...")
    while True:
        try:
            event = await event_queue.get()
            await process_event(event)
            event_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Consumer worker stopped.")
            break
        except Exception as e:
            logger.error(f"Error processing event: {e}")
