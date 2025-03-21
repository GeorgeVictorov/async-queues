import asyncio
from asyncio import LifoQueue, Queue
from dataclasses import dataclass, field


@dataclass(order=True)
class WorkItem:
    priority: int
    order: int
    data: str = field(compare=False)


async def worker(queue: Queue):
    while not queue.empty():
        work_item: WorkItem = await queue.get()
        print(f'Processing {work_item}')
        await asyncio.sleep(1)
        queue.task_done()


async def main():
    lifo_queue = LifoQueue()

    work_items = [
        WorkItem(3, 1, 'Lowest priority first'),
        WorkItem(3, 2, 'Lowest priority second'),
        WorkItem(3, 3, 'Lowest priority third'),
        WorkItem(2, 4, 'Medium priority'),
        WorkItem(1, 5, 'High priority')
    ]

    for work in work_items:
        lifo_queue.put_nowait(work)

    worker_tasks = asyncio.create_task(worker(lifo_queue))

    await asyncio.gather(lifo_queue.join(), worker_tasks)


if __name__ == '__main__':
    asyncio.run(main())
