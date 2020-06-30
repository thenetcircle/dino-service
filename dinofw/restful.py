import logging
from typing import List

from fastapi import FastAPI

from dinofw import environ
from dinofw.rest.models import HistoryQuery, Message, Histories, PaginationQuery, GroupUsers, UserStats, UserGroupStats, \
    Group, SearchQuery

logger = logging.getLogger(__name__)
logging.getLogger("amqp").setLevel(logging.INFO)
logging.getLogger("kafka.conn").setLevel(logging.INFO)


def create_app():
    return FastAPI()


app = create_app()


@app.post("/v1/groups/{group_id}/histories/{user_id}", response_model=List[Histories])
async def group_history_for_user(group_id: str, user_id: int, query: HistoryQuery) -> List[Histories]:
    return await environ.env.rest.group.histories(group_id, user_id, query)


@app.post("/v1/groups/{group_id}/messages", response_model=List[Message])
async def group_messages(group_id: str, query: HistoryQuery) -> List[Message]:
    return await environ.env.rest.group.messages(group_id, query)


@app.post("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def group_messages_for_user(group_id: str, user_id: int, query: HistoryQuery) -> List[Message]:
    return await environ.env.rest.group.messages_for_user(group_id, user_id, query)


@app.get("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}", response_model=Message)
async def get_message_details(group_id: str, user_id: int, message_id: str) -> Message:
    return await environ.env.rest.group.message(group_id, user_id, message_id)


@app.post("/v1/groups/{group_id}/users", response_model=GroupUsers)
async def users_in_group(group_id: str, query: PaginationQuery) -> GroupUsers:
    return await environ.env.rest.user.users(group_id, query)


@app.get("/v1/userstats/{user_id}", response_model=UserStats)
async def user_statistics(user_id: int) -> UserStats:
    return await environ.env.rest.user.stats(user_id)


@app.get("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def user_statistics_in_group(group_id: str, user_id: int) -> UserGroupStats:
    return await environ.env.rest.group.stats(group_id, user_id)


@app.post("/v1/groups", response_model=List[Group])
async def search_groups(query: SearchQuery) -> List[Group]:
    return await environ.env.rest.group.search(query)


@app.get("/v1/groups/{group_id}", response_model=Group)
async def group_information(group_id) -> Group:
    return await environ.env.rest.group.get_group(group_id)


@app.post("/v1/users/{user_id}/groups", response_model=List[Group])
async def groups_for_user(user_id: int) -> List[Group]:
    return await environ.env.rest.user.groups(user_id)
