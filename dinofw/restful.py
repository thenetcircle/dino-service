import logging
from typing import List

from fastapi import FastAPI

from dinofw import environ
from dinofw.rest.models import HistoryQuery, Message, Histories

logger = logging.getLogger(__name__)
logging.getLogger("amqp").setLevel(logging.INFO)
logging.getLogger("kafka.conn").setLevel(logging.INFO)


def create_app():
    return FastAPI()


app = create_app()


@app.post("/v1/groups/{group_id}/histories/{user_id}", response_model=List[Histories])
async def group_history_for_user(group_id: str, user_id: int, query: HistoryQuery) -> List[Histories]:
    return await environ.env.rest.group.history(group_id, user_id, query)


@app.post("/v1/groups/{group_id}/messages", response_model=List[Message])
async def group_messages(group_id: str, query: HistoryQuery) -> List[Message]:
    return await environ.env.rest.group.messages(group_id, query)
