import logging
from typing import List

from fastapi import FastAPI

from dinofw import environ
from dinofw.rest.models import AdminQuery, JoinerUpdateQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import EditMessageQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinQuery
from dinofw.rest.models import GroupJoinerQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import HistoryQuery
from dinofw.rest.models import Joiner
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import PaginationQuery
from dinofw.rest.models import SearchQuery
from dinofw.rest.models import SendMessageQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UserGroupStats
from dinofw.rest.models import UserStats

logger = logging.getLogger(__name__)
logging.getLogger("amqp").setLevel(logging.INFO)
logging.getLogger("kafka.conn").setLevel(logging.INFO)


def create_app():
    return FastAPI()


app = create_app()


@app.post("/v1/groups", response_model=List[Group])
async def search_groups(query: SearchQuery) -> List[Group]:
    """
    search groups sort by created time descendent
    """
    return await environ.env.rest.group.search(query)


@app.post("/v1/groups/{group_id}/histories/{user_id}", response_model=List[Histories])
async def group_history_for_user(group_id: str, user_id: int, query: HistoryQuery) -> List[Histories]:
    """
    get user visible history in a group sort by time in descendent, messages and action log.
    """
    return await environ.env.rest.group.histories(group_id, user_id, query)


@app.delete("/v1/groups/{group_id}/histories/{user_id}")
async def hide_group_history_for_user(group_id: str, user_id: int, query: HistoryQuery):
    """
    user hide group history, which won't affect other user(s), only mark for this user
    """
    return await environ.env.rest.group.hides_histories_for_user(group_id, user_id, query)


@app.post("/v1/groups/{group_id}/messages", response_model=List[Message])
async def group_messages(group_id: str, query: HistoryQuery) -> List[Message]:
    """
    get messages in a group, order by time in descendent
    """
    return await environ.env.rest.group.messages(group_id, query)


@app.put("/v1/groups/{group_id}/messages")
async def batch_update_messages(group_id: str, query: HistoryQuery):
    """
    batch update messages
    """
    return await environ.env.rest.group.update_messages(group_id, query)


@app.delete("/v1/groups/{group_id}/messages")
async def batch_delete_messages(group_id: str, query: HistoryQuery):
    """
    batch delete messages
    """
    return await environ.env.rest.group.delete_messages(group_id, query)


@app.post("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def group_messages_for_user(group_id: str, user_id: int, query: HistoryQuery) -> List[Message]:
    """
    get user messages in a group
    """
    return await environ.env.rest.group.messages_for_user(group_id, user_id, query)


@app.put("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def batch_update_messages_in_group_for_user(group_id: str, user_id: int, query: MessageQuery) -> List[Message]:
    """
    batch update user messages in a group (blocked, spammer, forcefakechecked)
    """
    return await environ.env.rest.group.messages_for_user(group_id, user_id, query)


@app.delete("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def batch_delete_messages_in_group_for_user(group_id: str, user_id: int, query: AdminQuery) -> List[Message]:
    """
    batch delete user messages in a group (gdpr)
    """
    return await environ.env.rest.group.delete_user_messages_for_group(group_id, user_id, query)


@app.post("/v1/groups/{group_id}/users/{user_id}/send", response_model=List[Message])
async def send_message_to_group(group_id: str, user_id: int, query: SendMessageQuery) -> List[Message]:
    """
    user sends a message in a group
    """
    return await environ.env.rest.message.send(group_id, user_id, query)


@app.get("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}", response_model=Message)
async def get_message_details(group_id: str, user_id: int, message_id: str) -> Message:
    """
    get message detail
    """
    return await environ.env.rest.message.details(group_id, user_id, message_id)


@app.put("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}", response_model=Message)
async def edit_message(group_id: str, user_id: int, message_id: str, query: EditMessageQuery) -> Message:
    """
    edit a group message
    """
    return await environ.env.rest.message.edit(group_id, user_id, message_id, query)


@app.delete("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}", response_model=Message)
async def delete_message(group_id: str, user_id: int, message_id: str, query: AdminQuery) -> Message:
    """
    delete a message in group (hard delete)
    """
    return await environ.env.rest.message.delete(group_id, user_id, message_id, query)


@app.post("/v1/groups/{group_id}/users", response_model=GroupUsers)
async def users_in_group(group_id: str, query: PaginationQuery) -> GroupUsers:
    """
    get users in group
    """
    return await environ.env.rest.group.users(group_id, query)


@app.get("/v1/groups/{group_id}", response_model=Group)
async def group_information(group_id) -> Group:
    """
    get group detail
    """
    return await environ.env.rest.group.get_group(group_id)


@app.put("/v1/groups/{group_id}", response_model=Group)
async def edit_group_information(group_id, query: UpdateGroupQuery) -> Group:
    """
    admin update group
    """
    return await environ.env.rest.group.edit(group_id)


@app.post("/v1/users/{user_id}/groups", response_model=List[Group])
async def groups_for_user(user_id: int, query: GroupQuery) -> List[Group]:
    """
    get user's group sort by latest message update
    """
    return await environ.env.rest.user.groups(user_id, query)


@app.post("/v1/users/{user_id}/groups/create", response_model=Group)
async def create_new_group(user_id: int, query: CreateGroupQuery) -> Group:
    """
    create a group
    """
    return await environ.env.rest.groups.create(user_id, query)


@app.put("/v1/users/{user_id}/groups/{group_id}", response_model=Group)
async def update_group(user_id: int, group_id: str, query: UpdateGroupQuery) -> Group:
    """
    update a group
    """
    return await environ.env.rest.groups.update(user_id, group_id, query)


@app.delete("/v1/users/{user_id}/groups/{group_id}")
async def delete_one_group_for_user(user_id: int, group_id: str):
    """
    owner delete a group
    """
    # TODO: how would deletion work here for other users in the group?
    return await environ.env.rest.user.delete(user_id, group_id)


@app.delete("/v1/users/{user_id}/groups")
async def delete_all_groups_for_user(user_id: int) -> Group:
    """
    batch delete user created group
    """
    # TODO: really delete all user's groups? what about other users in group?
    return await environ.env.rest.groups.delete(user_id)


@app.post("/v1/users/{user_id}/groups/{group_id}/joins", response_model=List[Joiner])
async def get_group_join_requests(user_id: int, group_id: str, query: GroupJoinerQuery) -> List[Joiner]:
    """
    get a group's join requests sort by create time in decendent
    """
    return await environ.env.rest.group.joins(user_id, group_id, query)


@app.put("/v1/users/{user_id}/groups/{group_id}/joins", response_model=List[Joiner])
async def get_group_join_requests(user_id: int, group_id: str, query: GroupJoinQuery) -> List[Joiner]:
    """
    send a group join request
    """
    return await environ.env.rest.group.joins(user_id, group_id, query)


@app.get("/v1/users/{user_id}/groups/{group_id}/joins/{joiner_id}", response_model=Joiner)
async def get_join_details(user_id: int, group_id: str, joiner_id: int) -> Joiner:
    """
    get join details
    """
    return await environ.env.rest.group.get_join_details(user_id, group_id, joiner_id)


@app.put("/v1/users/{user_id}/groups/{group_id}/joins/{joiner_id}", response_model=Joiner)
async def approve_or_deny_join_request(user_id: int, group_id: str, joiner_id: int, query: JoinerUpdateQuery) -> Joiner:
    """
    approve or deny a user join request
    """
    return await environ.env.rest.group.update_join_request(user_id, group_id, joiner_id, query)


@app.delete("/v1/users/{user_id}/groups/{group_id}/joins/{joiner_id}")
async def delete_join_request(user_id: int, group_id: str, joiner_id: int) -> None:
    """
    approve or deny a user join request
    """
    return await environ.env.rest.group.delete_join_request(user_id, group_id, joiner_id)


@app.get("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def user_statistics_in_group(group_id: str, user_id: int) -> UserGroupStats:
    """
    get user statistic in group
    """
    return await environ.env.rest.group.stats(group_id, user_id)


@app.get("/v1/userstats/{user_id}", response_model=UserStats)
async def user_statistics(user_id: int) -> UserStats:
    """
    get user statistic data
    """
    return await environ.env.rest.user.stats(user_id)
