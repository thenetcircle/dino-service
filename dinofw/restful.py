import logging
from typing import List

from fastapi import Depends
from fastapi import FastAPI
from sqlalchemy.orm import Session

from dinofw import environ
from dinofw.rest.models import ActionLog
from dinofw.rest.models import CreateActionLogQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import SendMessageQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateHighlightQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.rest.models import UserGroupStats
from dinofw.rest.models import UserStats

logger = logging.getLogger(__name__)
logging.getLogger("cassandra").setLevel(logging.INFO)
logging.getLogger("gmqtt").setLevel(logging.WARNING)


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


@app.post("/v1/groups/{group_id}/user/{user_id}/histories", response_model=Histories)
async def get_group_history_for_user(
    group_id: str, user_id: int, query: MessageQuery, db: Session = Depends(get_db)
) -> Histories:
    """
    get user visible history in a group sort by time in descendent, messages and action log.
    """
    return await environ.env.rest.group.histories(group_id, user_id, query, db)


@app.post("/v1/groups/{group_id}/users/{user_id}/send", response_model=Message)
async def send_message_to_group(
    group_id: str, user_id: int, query: SendMessageQuery, db: Session = Depends(get_db)
) -> List[Message]:
    """
    User sends a message in a group. This API should also be used for **1-to-1** conversations,
    if the client knows the `group_id` for the **1-to-1** conversations; otherwise the
    `POST /v1/users/{user_id}/send` API can be used to send a message and get the `group_id`.
    """
    return await environ.env.rest.message.send_message_to_group(group_id, user_id, query, db)


@app.post("/v1/users/{user_id}/send", response_model=Message)
async def send_message_to_user(
    user_id: int, query: SendMessageQuery, db: Session = Depends(get_db)
) -> List[Message]:
    """
    User sends a message in a **1-to-1** conversation. It is not always known on client side if a
    **1-to-1** group exists between two users, so this API can then be used; Dino will do a group
    lookup and see if a group with `group_type=1` exists for them, send a message to it and return
    the group_id.

    If no group exists, Dino will create a __new__ **1-to-1** group, send the message and return the
    `group_id`.

    This API should NOT be used for EVERY **1-to-1** message. It should only be used if the client
    doesn't know if a group exists for them or not. After this API has been called once, the client
    should use the `POST /v1/groups/{group_id}/users/{user_id}/send` API for future messages as
    much as possible.

    When listing recent history, the client will know the group_id for recent **1-to-1** conversations
    (since the groups that are **1-to-1** have `group_type=1`), and should thus use the other send API.
    """
    return await environ.env.rest.message.send_message_to_user(user_id, query, db)


@app.get("/v1/groups/{group_id}/users", response_model=GroupUsers)
async def get_users_in_group(
    group_id: str, db: Session = Depends(get_db)
) -> GroupUsers:
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
async def edit_group_information(group_id, query: UpdateGroupQuery, db: Session = Depends(get_db)) -> Group:
    """
    update group
    """
    return await environ.env.rest.group.update_group_information(group_id, query, db)


@app.post("/v1/users/{user_id}/groups", response_model=List[Group])
async def get_groups_for_user(
    user_id: int, query: GroupQuery, db: Session = Depends(get_db)
) -> List[Group]:
    """
    get user's group sort by latest message update
    """
    return await environ.env.rest.user.get_groups_for_user(user_id, query, db)


@app.post("/v1/users/{user_id}/groups/create", response_model=Group)
async def create_a_new_group(
    user_id: int, query: CreateGroupQuery, db: Session = Depends(get_db)
) -> Group:
    """
    create a group
    """
    return await environ.env.rest.group.create_new_group(user_id, query, db)


@app.put("/v1/groups/{group_id}/users/{user_id}/join")
async def join_group(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> None:
    """
    join a group
    """
    return await environ.env.rest.group.join_group(group_id, user_id, db)


@app.put("/v1/groups/{group_id}/users/{user_id}/highlight")
async def update_highlight_time(
        group_id: str,
        user_id: int,
        query: UpdateHighlightQuery,
        db: Session = Depends(get_db)
) -> None:
    """
    update highlight time of a group for another user
    """
    return await environ.env.rest.user.update_highlight_time(group_id, user_id, query, db)


@app.put("/v1/groups/{group_id}/actions", response_model=List[ActionLog])
async def create_action_logs(group_id: str, query: CreateActionLogQuery) -> None:
    """
    create actions logs in group
    """
    return await environ.env.rest.group.create_action_logs(group_id, query)


@app.delete("/v1/groups/{group_id}/users/{user_id}/join")
async def leave_group(
    user_id: int, group_id: str, db: Session = Depends(get_db)
) -> None:
    """
    leave a group
    """
    return await environ.env.rest.group.leave_group(group_id, user_id, db)


@app.get("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def get_user_statistics_in_group(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> UserGroupStats:
    """
    get user statistic in group
    """
    return await environ.env.rest.group.get_user_group_stats(group_id, user_id, db)


@app.put("/v1/groups/{group_id}/userstats/{user_id}")
async def update_user_statistics_in_group(
    group_id: str,
    user_id: int,
    query: UpdateUserGroupStats,
    db: Session = Depends(get_db),
) -> None:
    """
    update user statistic in group
    """
    return await environ.env.rest.group.update_user_group_stats(
        group_id, user_id, query, db
    )


@app.get("/v1/userstats/{user_id}", response_model=UserStats)
async def get_user_statistics(user_id: int, db: Session = Depends(get_db)) -> UserStats:
    """
    get user statistic data
    """
    return await environ.env.rest.user.get_user_stats(user_id, db)


@app.on_event("startup")
async def startup():
    await environ.env.publisher.setup()


"""
# not needed for now

@app.get(
    "/v1/groups/{group_id}/users/{user_id}/messages/{message_id}",
    response_model=Message,
)
async def get_message_details(group_id: str, user_id: int, message_id: str) -> Message:
    # get message details
    return await environ.env.rest.message.message_details(group_id, user_id, message_id)
"""


"""
# not needed for now

@app.put(
    "/v1/groups/{group_id}/users/{user_id}/messages/{message_id}",
    response_model=Message,
)
async def edit_a_message(
    group_id: str, user_id: int, message_id: str, query: EditMessageQuery
) -> Message:
    # edit a group message
    # TODO: handle no such message error
    return await environ.env.rest.message.edit_message(
        group_id, user_id, message_id, query
    )
"""


"""
# not needed for now

@app.delete("/v1/groups/{group_id}/users/{user_id}/messages/{message_id}")
async def delete_a_message(
    group_id: str, user_id: int, message_id: str, query: AdminQuery
) -> None:
    # delete a message in group (hard delete)
    # TODO: handle no such message error
    return await environ.env.rest.message.delete_message(
        group_id, user_id, message_id, query
    )
"""

"""
# not really necessary

@app.post(
    "/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message]
)
async def get_messages_for_user_in_group(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Message]:
    # TODO: get user messages in a group
    # TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    # TODO: see if we can iterate over all to find the user's messages
    return await environ.env.rest.message.messages_for_user(group_id, user_id, query)
"""


"""
# not useful; should not specify group_id when batch updating because of fakecheck etc.

@app.put("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def batch_update_messages_in_group_for_user(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Message]:
    # TODO: batch update user messages in a group (blocked, spammer, forcefakechecked)
    # TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    # TODO: see if we can iterate over all to find the user's messages then batch update
    return await environ.env.rest.message.update_messages_for_user_in_group(
        group_id, user_id, query
    )
"""

"""
# should not specify group, should be in all groups

@app.delete(
    "/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message]
)
async def batch_delete_messages_in_group_for_user(
    group_id: str, user_id: int, query: AdminQuery
) -> List[Message]:
    # TODO: batch delete user messages in a group (gdpr)
    # TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    # TODO: see if we can iterate over all to find the user's messages then batch delete
    return await environ.env.rest.message.delete_messages_for_user_in_group(
        group_id, user_id, query
    )
"""

# TODO: search groups sort by created time descendent
"""
@app.post("/v1/groups", response_model=List[Group])
async def search_for_groups(query: SearchQuery) -> List[Group]:
    return await environ.env.rest.group.search(query)
"""


"""
@app.delete("/v1/users/{user_id}/groups/{group_id}")
async def delete_one_group_for_user(user_id: int, group_id: str):
    # TODO: owner delete a group
    # TODO: this is just hiding right? use the update user group stats api instead
    # TODO: how would deletion work here for other users in the group?
    return await environ.env.rest.group.delete_on_group_for_user(user_id, group_id)
"""


"""
@app.delete("/v1/users/{user_id}/groups")
async def delete_all_groups_for_user(user_id: int) -> Group:
    # TODO: batch delete user created group
    # TODO: when would this ever be used?
    # TODO: really delete all user's groups? what about other users in group?
    return await environ.env.rest.groups.delete_all_groups_for_user(user_id)
"""