import logging
from typing import List

from fastapi import Depends
from fastapi import FastAPI
from sqlalchemy.orm import Session

from dinofw import environ
from dinofw.rest.models import AdminQuery
from dinofw.rest.models import AdminUpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.rest.models import ActionLog
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import EditMessageQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
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


# dependency
def get_db():
    db = environ.env.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/v1/groups", response_model=List[Group])
async def search_for_groups(query: SearchQuery) -> List[Group]:
    """
    TODO: search groups sort by created time descendent
    """
    return await environ.env.rest.group.search(query)


@app.post("/v1/groups/{group_id}/histories", response_model=List[Histories])
async def get_group_history_for_user(group_id: str, query: MessageQuery) -> List[Histories]:
    """
    get user visible history in a group sort by time in descendent, messages and action log.
    """
    return await environ.env.rest.group.histories(group_id, query)


@app.post("/v1/groups/{group_id}/messages", response_model=List[Message])
async def get_messages_in_group(group_id: str, query: MessageQuery) -> List[Message]:
    """
    get messages in a group, order by time in descendent
    """
    return await environ.env.rest.message.messages_in_group(group_id, query)


@app.put("/v1/groups/{group_id}/messages")
async def batch_update_messages_in_group(group_id: str, query: MessageQuery):
    """
    batch update messages in group
    """
    return await environ.env.rest.message.update_messages(group_id, query)


@app.delete("/v1/groups/{group_id}/messages")
async def batch_delete_messages_in_group(group_id: str, query: MessageQuery):
    """
    batch delete messages in group
    """
    return await environ.env.rest.message.delete_messages(group_id, query)


@app.post("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def get_messages_for_user_in_group(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Message]:
    """
    TODO: get user messages in a group
    TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    TODO: see if we can iterate over all to find the user's messages
    """
    return await environ.env.rest.message.messages_for_user(group_id, user_id, query)


@app.put("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def batch_update_messages_in_group_for_user(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Message]:
    """
    TODO: batch update user messages in a group (blocked, spammer, forcefakechecked)
    TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    TODO: see if we can iterate over all to find the user's messages then batch update
    """
    return await environ.env.rest.message.update_messages_for_user_in_group(
        group_id, user_id, query
    )


@app.delete(
    "/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message]
)
async def batch_delete_messages_in_group_for_user(
    group_id: str, user_id: int, query: AdminQuery
) -> List[Message]:
    """
    TODO: batch delete user messages in a group (gdpr)
    TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    TODO: see if we can iterate over all to find the user's messages then batch delete
    """
    return await environ.env.rest.message.delete_messages_for_user_in_group(
        group_id, user_id, query
    )


@app.post("/v1/groups/{group_id}/users/{user_id}/send", response_model=Message)
async def send_message_to_group(
    group_id: str, user_id: int, query: SendMessageQuery, db: Session = Depends(get_db)
) -> List[Message]:
    """
    user sends a message in a group
    """
    return await environ.env.rest.message.save_new_message(group_id, user_id, query, db)


@app.get("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}", response_model=Message)
async def get_message_details(group_id: str, user_id: int, message_id: str) -> Message:
    """
    get message details
    """
    return await environ.env.rest.message.message_details(group_id, user_id, message_id)


@app.put("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}", response_model=Message)
async def edit_a_message(
    group_id: str, user_id: int, message_id: str, query: EditMessageQuery
) -> Message:
    """
    edit a group message
    """
    # TODO: handle no such message error
    return await environ.env.rest.message.edit_message(group_id, user_id, message_id, query)


@app.delete("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}")
async def delete_a_message(
    group_id: str, user_id: int, message_id: str, query: AdminQuery
) -> None:
    """
    delete a message in group (hard delete)
    """
    # TODO: handle no such message error
    return await environ.env.rest.message.delete_message(group_id, user_id, message_id, query)


@app.get("/v1/groups/{group_id}/users", response_model=GroupUsers)
async def get_users_in_group(group_id: str, db: Session = Depends(get_db)) -> GroupUsers:
    """
    get users in group
    """
    # TODO: handle no such group error
    return await environ.env.rest.group.get_users_in_group(group_id, db)


@app.get("/v1/groups/{group_id}", response_model=Group)
async def get_group_information(group_id, db: Session = Depends(get_db)) -> Group:
    """
    get group details
    """
    return await environ.env.rest.group.get_group(group_id, db)


@app.put("/v1/groups/{group_id}")
async def edit_group_information(group_id, query: AdminUpdateGroupQuery) -> Group:
    """
    admin update group  TODO: failure/success response model
    """
    return await environ.env.rest.group.admin_update_group_information(group_id, query)


@app.post("/v1/users/{user_id}/groups", response_model=List[Group])
async def get_groups_for_user(user_id: int, query: GroupQuery, db: Session = Depends(get_db)) -> List[Group]:
    """
    get user's group sort by latest message update
    """
    return await environ.env.rest.user.get_groups_for_user(user_id, query, db)


@app.post("/v1/users/{user_id}/groups/create", response_model=Group)
async def create_a_new_group(user_id: int, query: CreateGroupQuery, db: Session = Depends(get_db)) -> Group:
    """
    create a group
    """
    return await environ.env.rest.group.create_new_group(user_id, query, db)


@app.put("/v1/users/{user_id}/groups/{group_id}")
async def update_group_information(
    user_id: int, group_id: str, query: UpdateGroupQuery, db: Session = Depends(get_db)
) -> Group:
    """
    update a group
    """
    return await environ.env.rest.group.update_group_information(user_id, group_id, query, db)


@app.delete("/v1/users/{user_id}/groups/{group_id}")
async def delete_one_group_for_user(user_id: int, group_id: str):
    """
    TODO: owner delete a group
    """
    # TODO: how would deletion work here for other users in the group?
    return await environ.env.rest.group.delete_on_group_for_user(user_id, group_id)


@app.delete("/v1/users/{user_id}/groups")
async def delete_all_groups_for_user(user_id: int) -> Group:
    """
    TODO: batch delete user created group
    """
    # TODO: really delete all user's groups? what about other users in group?
    return await environ.env.rest.groups.delete_all_groups_for_user(user_id)


@app.put("/v1/groups/{group_id}/users/{user_id}/join", response_model=ActionLog)
async def join_group(group_id: str, user_id: int, db: Session = Depends(get_db)) -> ActionLog:
    """
    join a group
    """
    return await environ.env.rest.group.join_group(group_id, user_id, db)


@app.delete("/v1/groups/{group_id}/users/{user_id}/join")
async def leave_group(user_id: int, group_id: str, db: Session = Depends(get_db)) -> None:
    """
    leave a group
    """
    return await environ.env.rest.group.leave_group(group_id, user_id, db)


@app.get("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def get_user_statistics_in_group(
        group_id: str,
        user_id: int,
        db: Session = Depends(get_db)
) -> UserGroupStats:
    """
    get user statistic in group
    """
    return await environ.env.rest.group.get_user_group_stats(group_id, user_id, db)


@app.put("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def update_user_statistics_in_group(
        group_id: str,
        user_id: int,
        query: UpdateUserGroupStats,
        db: Session = Depends(get_db)
) -> UserGroupStats:
    """
    update user statistic in group
    """
    return await environ.env.rest.group.update_user_group_stats(group_id, user_id, query, db)


@app.get("/v1/userstats/{user_id}", response_model=UserStats)
async def get_user_statistics(user_id: int, db: Session = Depends(get_db)) -> UserStats:
    """
    get user statistic data
    """
    return await environ.env.rest.user.get_user_stats(user_id, db)
