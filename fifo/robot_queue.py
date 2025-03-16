import asyncio
import logging
from asyncio import Queue

from aiohttp import ClientSession
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class WorkItem:
    def __init__(self, item_depth: int, url: str):
        self.item_depth = item_depth
        self.url = url


async def worker(worker_id: int, queue: Queue, session: ClientSession, max_depth: int):
    logger.info(f'Worker: {worker_id}')
    while True:
        work_item: WorkItem = await queue.get()
        logger.info(f'Worker: {worker_id} is processing {work_item.url}')
        await process_page(work_item, queue, session, max_depth)
        logger.info(f'Worker: {worker_id} has finished processing {work_item.url}')
        queue.task_done()


async def process_page(work_item: WorkItem, queue: Queue, session: ClientSession, max_depth: int):
    try:
        response = await asyncio.wait_for(session.get(work_item.url), timeout=3)
        if work_item.item_depth == max_depth:
            logger.info(f'Max depth reached for {work_item.url}')
        else:
            body = await response.text()
            soup = BeautifulSoup(body, 'html.parser')
            links = soup.find_all('a', href=True)
            for link in links:
                queue.put_nowait(WorkItem(work_item.item_depth + 1, link['href']))
    except Exception:
        pass
        # logger.exception(f'Exception while processing {work_item.url}')


async def main():
    start_url = 'https://example.com'
    url_queue = Queue()
    url_queue.put_nowait(WorkItem(0, start_url))
    async with ClientSession() as session:
        workers = [asyncio.create_task(worker(i, url_queue, session, 3)) for i in range(100)]
        await url_queue.join()
        [w.cancel() for w in workers]


if __name__ == '__main__':
    asyncio.run(main())
