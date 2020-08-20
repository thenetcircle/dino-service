import asyncio
import logging

from fastapi import FastAPI

from dinofw.client_environ import env
from dinofw.config import ConfigKeys
from dinofw.endpoint.stream import StreamReader

logger = logging.getLogger(__name__)
logging.basicConfig(level="DEBUG", format=ConfigKeys.DEFAULT_LOG_FORMAT)

app = FastAPI()

pub_host, pub_port = env.config.get(ConfigKeys.HOST, domain=ConfigKeys.PUBLISHER).split(":", 1)
pub_db = env.config.get(ConfigKeys.DB, domain=ConfigKeys.PUBLISHER, default=0)

reader = StreamReader(env, pub_host, pub_port, pub_db)


async def read():
    await reader.setup()

    while True:
        try:
            await reader.consume()
        except (InterruptedError, asyncio.CancelledError) as e:
            raise e


loop = asyncio.get_event_loop()
asyncio.ensure_future(read(), loop=loop)
