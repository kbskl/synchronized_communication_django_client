import asyncio
import json
import threading
import uuid
import aioredis
import async_timeout
from util.multiple_server_manager_config import ROUTER_CHANNEL, REDIS_URL, REDIS_PASSWORD


class MultipleServerManager:
    def __init__(self):
        self.redis = None
        self.pubsub = None
        self.router_channel = ROUTER_CHANNEL

    async def __load(self):
        self.redis = aioredis.from_url(REDIS_URL, password=REDIS_PASSWORD)
        self.pubsub = self.redis.pubsub()

    def __generate_uuid(self):
        return uuid.uuid4()

    async def __reader(self, channel: aioredis.client.PubSub, channels_functions_dict: dict):
        print("Reader starting...")
        while True:
            try:
                async with async_timeout.timeout(1):
                    message = await channel.get_message(ignore_subscribe_messages=True)
                    if message is not None:
                        data = json.loads(message['data'].decode())
                        if data.get('sender', None) or data.get('transaction_uuid', None):
                            continue
                        await self.__publish(self.router_channel, json.dumps({"transaction_uuid": data['uuid']}))
                        if channels_functions_dict.get(data['from'], None):
                            channels_functions_dict[data['from']](data['data'])
                    await asyncio.sleep(0.01)
            except aioredis.exceptions.ConnectionError:
                print("Connection error. Sub Closed.")
                await asyncio.sleep(10)
                break
            except Exception as e:
                print(f"Error:{e}")
                continue

    def subscribe(self, channels_functions_dict=None):
        def loop_in_thread(lp, channels_functions_dict):
            asyncio.set_event_loop(lp)
            lp.run_until_complete(self.__subscribe(channels_functions_dict))

        channels_functions_dict = channels_functions_dict if channels_functions_dict else {}
        loop = asyncio.new_event_loop()
        t = threading.Thread(target=loop_in_thread, args=(loop, channels_functions_dict,))
        t.start()

    async def __subscribe(self, channels_functions_dict):
        await self.__load()
        await self.pubsub.subscribe(self.router_channel)
        future = asyncio.create_task(self.__reader(self.pubsub, channels_functions_dict=channels_functions_dict))
        await future

    def publish(self, server, data):
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError as e:
            if str(e).startswith('There is no current event loop in thread'):
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            else:
                raise e
        asyncio.run(self.__publish(self.router_channel,
                                   json.dumps({"to": server, "sender": self.router_channel, "data": data,
                                               "uuid": f"{self.__generate_uuid()}"})))

    async def __publish(self, channel, data):
        await self.__load()
        await self.redis.publish(channel, data)

