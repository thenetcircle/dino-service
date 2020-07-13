import logging
from typing import List

from fastapi import Depends
from fastapi import FastAPI
from sqlalchemy.orm import Session

from dinofw import environ
from dinofw.rest.models import AdminQuery, JoinerUpdateQuery, AdminUpdateGroupQuery, UpdateUserGroupStats
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import EditMessageQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupJoinQuery
from dinofw.rest.models import GroupJoinerQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Joiner
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
async def get_group_history_for_user(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Histories]:
    """
    TODO: get user visible history in a group sort by time in descendent, messages and action log.
    """
    return await environ.env.rest.group.histories(group_id, user_id, query)


@app.delete("/v1/groups/{group_id}/histories/{user_id}")
async def hide_group_history_for_user(group_id: str, user_id: int, query: MessageQuery):
    """
    TODO: user hide group history, which won't affect other user(s), only mark for this user
    TODO: this might not be needed anymore, since we'll be using `hide_before` in the query
    """
    return await environ.env.rest.group.hide_histories_for_user(
        group_id, user_id, query
    )


@app.post("/v1/groups/{group_id}/messages", response_model=List[Message])
async def get_messages_in_group(group_id: str, query: MessageQuery) -> List[Message]:
    """
    get messages in a group, order by time in descendent
    """
    return await environ.env.rest.message.messages_in_group(group_id, query)


@app.put("/v1/groups/{group_id}/messages")
async def batch_update_messages_in_group(group_id: str, query: MessageQuery):
    """
    TODO: batch update messages in group
    """
    return await environ.env.rest.message.update_messages(group_id, query)


@app.delete("/v1/groups/{group_id}/messages")
async def batch_delete_messages_in_group(group_id: str, query: MessageQuery):
    """
    TODO: batch delete messages in group
    """
    return await environ.env.rest.message.delete_messages(group_id, query)


@app.post(
    "/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message]
)
async def get_messages_for_user_in_group(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Message]:
    """
    get user messages in a group
    """
    return await environ.env.rest.message.messages_for_user(group_id, user_id, query)


@app.put("/v1/groups/{group_id}/users/{user_id}/messages", response_model=List[Message])
async def batch_update_messages_in_group_for_user(
    group_id: str, user_id: int, query: MessageQuery
) -> List[Message]:
    """
    TODO: batch update user messages in a group (blocked, spammer, forcefakechecked)
    """
    return await environ.env.rest.message.update_messages_for_user(
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


@app.get(
    "/v1/groups/{group_id}/users/{user_id}/messages/{message_id}",
    response_model=Message,
)
async def get_message_details(group_id: str, user_id: int, message_id: str) -> Message:
    """
    TODO: get message details
    """
    return await environ.env.rest.message.details(group_id, user_id, message_id)


@app.put(
    "/v1/groups/{group_id}/users/{user_id}/messages/{message_id}",
    response_model=Message,
)
async def edit_a_message(
    group_id: str, user_id: int, message_id: str, query: EditMessageQuery
) -> Message:
    """
    TODO: edit a group message
    """
    return await environ.env.rest.message.edit(group_id, user_id, message_id, query)


@app.delete(
    "/v1/groups/{group_id}/users/{user_id}/messages/{message_id}",
    response_model=Message,
)
async def delete_a_message(
    group_id: str, user_id: int, message_id: str, query: AdminQuery
) -> Message:
    """
    TODO: delete a message in group (hard delete)
    """
    return await environ.env.rest.message.delete(group_id, user_id, message_id, query)


@app.get("/v1/groups/{group_id}/users", response_model=GroupUsers)
async def get_users_in_group(group_id: str, db: Session = Depends(get_db)) -> GroupUsers:
    """
    get users in group
    """
    return await environ.env.rest.group.get_users_in_group(group_id, db)


@app.get("/v1/groups/{group_id}", response_model=Group)
async def get_group_information(group_id, db: Session = Depends(get_db)) -> Group:
    """
    get group details
    """
    return await environ.env.rest.group.get_group(group_id, db)


@app.put("/v1/groups/{group_id}", response_model=Group)
async def edit_group_information(group_id, query: AdminUpdateGroupQuery) -> Group:
    """
    TODO: admin update group
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


@app.put("/v1/users/{user_id}/groups/{group_id}", response_model=Group)
async def update_group_information(
    user_id: int, group_id: str, query: UpdateGroupQuery
) -> Group:
    """
    TODO: update a group
    """
    return await environ.env.rest.groups.update_group_information(
        user_id, group_id, query
    )


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


@app.post("/v1/users/{user_id}/groups/{group_id}/joins", response_model=List[Joiner])
async def get_group_join_requests(
    user_id: int, group_id: str, query: GroupJoinerQuery
) -> List[Joiner]:
    """
    get a group's join requests sort by create time in decendent
    """
    return await environ.env.rest.group.get_join_requests(group_id, query)


@app.put("/v1/users/{user_id}/groups/{group_id}/joins", response_model=List[Joiner])
async def send_join_request_to_group(
    user_id: int, group_id: str, query: GroupJoinQuery
) -> List[Joiner]:
    """
    send a group join request TODO: also send to dino client?
    """
    return await environ.env.rest.group.save_join_request(group_id, query)


@app.get(
    "/v1/users/{user_id}/groups/{group_id}/joins/{joiner_id}", response_model=Joiner
)
async def get_group_join_details(user_id: int, group_id: str, joiner_id: int) -> Joiner:
    """
    get join details
    """
    return await environ.env.rest.group.get_join_details(user_id, group_id, joiner_id)


@app.put(
    "/v1/users/{user_id}/groups/{group_id}/joins/{joiner_id}", response_model=Joiner
)
async def approve_or_deny_group_join_request(
    user_id: int, group_id: str, joiner_id: int, query: JoinerUpdateQuery
) -> Joiner:
    """
    TODO: approve or deny a user join request
    """
    return await environ.env.rest.group.update_join_request(
        user_id, group_id, joiner_id, query
    )


@app.delete("/v1/users/{user_id}/groups/{group_id}/joins/{joiner_id}")
async def delete_group_join_request(
    user_id: int, group_id: str, joiner_id: int
) -> None:
    """
    TODO: approve or deny a user join request
    """
    return await environ.env.rest.group.delete_join_request(
        user_id, group_id, joiner_id
    )


@app.get("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def get_user_statistics_in_group(group_id: str, user_id: int) -> UserGroupStats:
    """
    TODO: get user statistic in group
    """
    return await environ.env.rest.group.get_stats(group_id, user_id)


@app.put("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def update_user_statistics_in_group(
        group_id: str,
        user_id: int,
        query: UpdateUserGroupStats
) -> UserGroupStats:
    """
    TODO: update user statistic in group
    """
    return await environ.env.rest.group.update_stats(group_id, user_id, query)


@app.get("/v1/userstats/{user_id}", response_model=UserStats)
async def get_user_statistics(user_id: int) -> UserStats:
    """
    TODO: get user statistic data
    """
    return await environ.env.rest.user.get_stats(user_id)
