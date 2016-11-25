import asyncio
import aioredis
from multiprocessing import Process
from multiprocessing.queues import Empty
from aioredis import RedisError

class SetRedisValue(Process):
    def __init__(self, redis_data_queue):
        Process.__init__(self)
        self.redis_data_queue = redis_data_queue
        self.redis_pool_list = {}

    def run(self):
        print("inside SetRedisValue Process")
        ioloop = asyncio.get_event_loop()
        ioloop.set_debug(enabled=True)
        ioloop.run_until_complete(asyncio.wait([self.fetch_queue_elements() for i in range(4)]))

    async def fetch_queue_elements(self):
        while True:
            try:
                item = await self.redis_data_queue.coro_get()
            except Empty:
                await asyncio.sleep(0.001)
                continue
            await self.process_item(item)

    async def process_item(self, item):
        redis_conn_info = ("localhost", 7777, 0)
        if redis_conn_info not in self.redis_pool_list:
            await self.get_connection_pool(host=redis_conn_info[0], port=redis_conn_info[1], db=redis_conn_info[2])
        redis_pool = self.redis_pool_list[redis_conn_info]
        with await redis_pool as redis_con:
            try:
                await redis_con.set(item, "")
            except RedisError as e:
                print("error occured:", e)

    async def get_connection_pool(self, host="localhost", port=6379, db=0, min_size=5, max_size=10):
        pool = await aioredis.create_pool((host, int(port)),db=int(db), minsize=min_size, maxsize=max_size, encoding="utf-8")
        self.redis_pool_list[(host, port, db)] = pool
