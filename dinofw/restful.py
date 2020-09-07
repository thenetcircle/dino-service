import inspect
import logging
import sys
from typing import List

from fastapi import Depends, HTTPException
from fastapi import FastAPI
from fastapi import status
from sqlalchemy.orm import Session

from dinofw import environ
from dinofw.config import ErrorCodes
from dinofw.rest.models import ActionLog
from dinofw.rest.models import CreateActionLogQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUpdatesQuery
from dinofw.rest.models import GroupUsers
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import OneToOneQuery
from dinofw.rest.models import OneToOneStats
from dinofw.rest.models import SendMessageQuery
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.rest.models import UserStats
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException

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
    Get user visible history in a group for a user. Sorted by creation time in descendent.
    Response contains a list of messages sent in the group, a list of action logs, and a list
    the last read time for each user in the group.

    Calling this API will update `last_read_time` in this group for this user, and
    broadcast the new read time to every one else in the group that is online.

    History can be filtered by `message_type` to e.g. only list images sent in the group.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.histories(group_id, user_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/groups/{group_id}/user/{user_id}/send", response_model=Message)
async def send_message_to_group(
    group_id: str, user_id: int, query: SendMessageQuery, db: Session = Depends(get_db)
) -> List[Message]:
    """
    User sends a message in a group. This API should also be used for **1-to-1** conversations
    if the client knows the `group_id` for the **1-to-1** conversations. Otherwise the
    `POST /v1/users/{user_id}/send` API can be used to send a message and get the `group_id`.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.send_message_to_group(group_id, user_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


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

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.send_message_to_user(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.get("/v1/groups/{group_id}/users", response_model=GroupUsers)
async def get_users_in_group(
    group_id: str, db: Session = Depends(get_db)
) -> GroupUsers:
    """
    Get a list of users in the group. The response will contain the owner of the group, and a list of
    user IDs and their join time, so clients can list users in order of joining.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.get_users_in_group(group_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.get("/v1/groups/{group_id}", response_model=Group)
async def get_group_information(group_id, db: Session = Depends(get_db)) -> Group:
    """
    Get details about one group.

    TODO: need another api for group info where the `group_id` might be unknown by clients; i.e. for 1v1 conversations

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.get_group(group_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.get("/v1/users/{user_id}/group", response_model=OneToOneStats)
async def get_one_to_one_information(
        user_id: int,
        query: OneToOneQuery,
        db: Session = Depends(get_db)
) -> OneToOneStats:
    """
    Get details about a 1v1 group.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.get_1v1_info(user_id, query.receiver_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.put("/v1/groups/{group_id}")
async def edit_group_information(group_id, query: UpdateGroupQuery, db: Session = Depends(get_db)) -> Group:
    """
    Update group details.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.update_group_information(group_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/users/{user_id}/groups", response_model=List[UserGroup])
async def get_groups_for_user(
    user_id: int, query: GroupQuery, db: Session = Depends(get_db)
) -> List[UserGroup]:
    """
    Get a list of groups for this user, sorted by last message sent. Can be filtered
    to only return groups where the user has unread messages.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_groups_for_user(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/users/{user_id}/groups/updates", response_model=List[UserGroup])
async def get_groups_updated_since(
    user_id: int, query: GroupUpdatesQuery, db: Session = Depends(get_db)
) -> List[UserGroup]:
    """
    Get a list of groups for this user that has changed since a certain time, sorted
    by last message sent. Used to sync changes to mobile apps.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_groups_updated_since(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/users/{user_id}/groups/create", response_model=Group)
async def create_a_new_group(
    user_id: int, query: CreateGroupQuery, db: Session = Depends(get_db)
) -> Group:
    """
    Create a new group. A list of user IDs can be specified to make them auto-join
    this new group.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.create_new_group(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.put("/v1/groups/{group_id}/user/{user_id}/join")
async def join_group(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> None:
    """
    Join a group.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.join_group(group_id, user_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.put("/v1/groups/{group_id}/actions", response_model=List[ActionLog])
async def create_action_logs(group_id: str, query: CreateActionLogQuery) -> None:
    """
    Create one or more action logs in group.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.create_action_logs(group_id, query)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.delete("/v1/groups/{group_id}/user/{user_id}/join")
async def leave_group(
    user_id: int, group_id: str, db: Session = Depends(get_db)
) -> None:
    """
    Leave a group.

    **Potential error codes in response:** 
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.leave_group(group_id, user_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.get("/v1/groups/{group_id}/userstats/{user_id}", response_model=UserGroupStats)
async def get_user_statistics_in_group(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> UserGroupStats:
    """
    Get a user's statistic in a group (last read, hidden, etc.).

    **Potential error codes in response:** 
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.get_user_group_stats(group_id, user_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.put("/v1/groups/{group_id}/user/{user_id}/update")
async def update_user_statistics_in_group(
    group_id: str,
    user_id: int,
    query: UpdateUserGroupStats,
    db: Session = Depends(get_db),
) -> None:
    """
    Update user statistic in a group. Only values specified in the query
    will be updated (if a field is blank in the query it won't be updated).

    This API should __NOT__ be used to update `last_read_time` when opening
    a conversation. The `last_read_time` is updated automatically when a
    user calls the `/v1/groups/{group_id}/user/{user_id}/histories` API for
    a group.

    **Can be used for updating the following:**

    * `last_read_time`: should be creating time of last received message,
    * `delete_before`: when a user deletes a conversation, set to the creation time of the last received message,
    * `highlight_time`: until when should this conversation be highlighted for this user,
    * `hide`: whether to hide/show a conversation,
    * `bookmark`: whether to bookmark a conversation or not,
    * `pin`: whether to pin a conversation or not,
    * `rating`: a user can rate a conversation (1v1 usually).

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.update_user_group_stats(
            group_id, user_id, query, db
        )
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.get("/v1/userstats/{user_id}", response_model=UserStats)
async def get_user_statistics(user_id: int, db: Session = Depends(get_db)) -> UserStats:
    """
    Get a user's statistics globally (not only for one group).

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_user_stats(user_id, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.put("/v1/users/{user_id}/messages", response_model=List[Message])
async def update_user_message_status(
        user_id: int,
        query: MessageQuery,
        db: Session = Depends(get_db)
) -> None:
    """
    Update user message status, e.g. because the user got blocked, is a bot,
    was force fake-checked, etc.

    * TODO: this is not easy to do in cassandra since created_at comes before user_id in the partition keys
    * TODO: see if we can iterate over all to find the user's messages then batch update

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.update_user_message_status(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.on_event("startup")
async def startup():
    await environ.env.publisher.setup()


def log_error_and_raise_unknown(exc_info, e):
    func_name = inspect.currentframe().f_back.f_code.co_name
    logger.error(f"{func_name}: {str(e)}")
    logger.exception(e)
    environ.env.capture_exception(exc_info)
    raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"{ErrorCodes.UNKNOWN_ERROR}: {str(e)}",
    )


def log_error_and_raise_known(error_code, e):
    details = f"{error_code}: {e.message}"
    logger.error(details)
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"{error_code}: {e.message}",
    )


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