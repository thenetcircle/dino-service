import inspect
import logging
import sys
from typing import List

from fastapi import Depends, HTTPException
from fastapi import FastAPI
from fastapi import status
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.responses import Response
from starlette.status import HTTP_201_CREATED

from dinofw.rest.models import CreateActionLogQuery, AttachmentQuery
from dinofw.rest.models import CreateAttachmentQuery
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
from fastapi.middleware.cors import CORSMiddleware
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.rest.models import UserStats
from dinofw.utils import environ
from dinofw.utils.config import ErrorCodes
from dinofw.utils.exceptions import NoSuchGroupException, NoSuchUserException, NoSuchAttachmentException, \
    NoSuchMessageException
from dinofw.utils.exceptions import UserNotInGroupException

logger = logging.getLogger(__name__)
logging.getLogger("cassandra").setLevel(logging.INFO)
logging.getLogger("gmqtt").setLevel(logging.WARNING)


def create_app():
    api = FastAPI()

    api.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return api


app = create_app()


# dependency
def get_db():
    db = environ.env.SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.post("/v1/users/{user_id}/groups", response_model=List[UserGroup])
async def get_groups_for_user(
    user_id: int, query: GroupQuery, db: Session = Depends(get_db)
) -> List[UserGroup]:
    """
    Get a list of groups for this user, sorted by last message sent. For paying users,
    the `count_unread` field can be set to True (default is False).

    If `count_unread` is False, the fields `unread` and `receiver_unread` will have
    the value `-1`.

    If `hidden` is set to True in the query, only hidden groups will be returned.
    Defaults value is False.

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


@app.post("/v1/users/{user_id}/group", response_model=OneToOneStats)
async def get_one_to_one_information(
    user_id: int, query: OneToOneQuery, db: Session = Depends(get_db)
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
    * `604`: if the user does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.send_message_to_user(user_id, query, db)
    except NoSuchUserException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_USER, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/users/{user_id}/message/{message_id}/attachment")
async def create_an_attachment(
        user_id: int, message_id: str, query: CreateAttachmentQuery, db: Session = Depends(get_db)
) -> None:
    """
    Create an attachment.

    When a user sends an image or video, first call the "send message API" with the
    `message_type` set to `image` (or similar). When the image has finished processing
    in the backend, call this API to create the actual attachment meta data for it.

    First we create the "empty" message so indicate to all relevant users that someone
    has sent something, usually the client application will show a loading icon for this
    "empty" message. When this API is called after the image processing is done, Dino
    will broadcast an update to the clients with a reference to the ID of the "empty"
    message, so the real image can replace the loading icon.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `602`: if the message does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.create_attachment(user_id, message_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except NoSuchMessageException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_MESSAGE, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/groups/{group_id}/attachment", response_model=Message)
async def get_attachment_info_from_file_id(
    group_id: str, query: AttachmentQuery, db: Session = Depends(get_db)
) -> Message:
    """
    Get attachment info from `file_id`.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `604`: if the attachment does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.get_attachment_info(group_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except NoSuchAttachmentException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_ATTACHMENT, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.post("/v1/groups/{group_id}/user/{user_id}/attachments", response_model=List[Message])
async def get_attachments_in_group_for_user(
    group_id: str, user_id: int, query: MessageQuery, db: Session = Depends(get_db)
) -> List[Message]:
    """
    Get all attachments in this group for this user.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.get_attachments_in_group_for_user(
            group_id, user_id, query, db
        )
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.delete("/v1/groups/{group_id}/attachment", status_code=HTTP_201_CREATED)
async def delete_attachment_with_file_id(
    group_id: str, query: AttachmentQuery, db: Session = Depends(get_db)
) -> Response:
    """
    Delete an attachment.

    # TODO: implement, async, send file_id to kafka on completion

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    def _delete_attachment_with_file_id(group_id_, query_, db_):
        environ.env.rest.group.delete_attachment(group_id_, query_, db_)

    try:
        task = BackgroundTask(_delete_attachment_with_file_id, group_id_=group_id, query_=query, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.delete("/v1/groups/{group_id}/user/{user_id}/attachments", status_code=HTTP_201_CREATED)
async def delete_attachments_in_group_for_user(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> Response:
    """
    Delete all attachments in this group for this user.

    # TODO: implement, async, send file_id to kafka on completion

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    def _delete_attachments_in_group_for_user(group_id_, user_id_, db_):
        environ.env.rest.group.delete_attachments_in_group_for_user(
            group_id_, user_id_, db_
        )

    try:
        task = BackgroundTask(_delete_attachments_in_group_for_user, group_id_=group_id, user_id_=user_id, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.delete("/v1/user/{user_id}/attachments", status_code=HTTP_201_CREATED)
async def delete_attachments_in_all_groups_from_user(user_id: int, db: Session = Depends(get_db)) -> Response:
    """
    Delete all attachments send by this user in all groups.

    # TODO: implement, async, send file_id to kafka on completion

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    def _delete_attachments_in_all_groups_from_user(user_id_, db_):
        environ.env.rest.user.delete_all_user_attachments(user_id_, db_)

    try:
        task = BackgroundTask(_delete_attachments_in_all_groups_from_user, user_id_=user_id, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


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
        return await environ.env.rest.message.send_message_to_group(
            group_id, user_id, query, db
        )
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, e)
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


@app.put("/v1/groups/{group_id}")
async def edit_group_information(
    group_id, query: UpdateGroupQuery, db: Session = Depends(get_db)
) -> Group:
    """
    Update group details.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.update_group_information(
            group_id, query, db
        )
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
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


@app.post("/v1/groups/{group_id}/actions", response_model=List[Message])
async def create_action_logs(
    group_id: str, query: CreateActionLogQuery, db: Session = Depends(get_db)
) -> None:
    """
    Create one or more action logs in group.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.create_action_logs(group_id, query, db)
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
        return environ.env.rest.group.leave_group(group_id, user_id, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.get("/v1/groups/{group_id}/user/{user_id}", response_model=UserGroupStats)
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
        message_amount = await environ.env.rest.group.count_messages_in_group(group_id)
        return await environ.env.rest.group.get_user_group_stats(group_id, user_id, message_amount, db)
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


@app.put("/v1/userstats/{user_id}", status_code=HTTP_201_CREATED)
async def update_user_stats(user_id: int, db: Session = Depends(get_db)) -> Response:
    """
    Update user status, e.g. because the user got blocked, is a bot, was
    force fake-checked, etc. Will set `last_updated_at` on all user group
    stats that has had an interaction with this user (including this
    user's user group stats).

    This API is run asynchronously, and returns a 201 Created instead of
    200 OK.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    def set_last_updated(user_id_, db_):
        environ.env.rest.group.set_last_updated_at_on_all_stats_related_to_user(
            user_id_, db_
        )

    try:
        task = BackgroundTask(set_last_updated, user_id_=user_id, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.put("/v1/user/{user_id}/read", status_code=HTTP_201_CREATED)
async def mark_all_groups_as_read(user_id: int, db: Session = Depends(get_db)) -> Response:
    """
    Mark all groups as read, including removing any bookmarks done by the
    user.

    This API is run asynchronously, and returns a 201 Created instead of
    200 OK.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    def set_read_time(user_id_, db_):
        environ.env.rest.group.mark_all_as_read(
            user_id_, db_
        )

    try:
        task = BackgroundTask(set_read_time, user_id_=user_id, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@app.delete("/v1/users/{user_id}/groups", status_code=HTTP_201_CREATED)
async def delete_all_groups_for_user(user_id: int, db: Session = Depends(get_db)) -> Response:
    """
    When a user removes his/her profile, make the user leave all groups.

    # TODO: discuss about deletion of messages; when? GDPR

    This API is run asynchronously, and returns a 201 Created instead of
    200 OK.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    def leave_all_groups(user_id_, db_):
        environ.env.rest.group.delete_all_groups_for_user(
            user_id_, db_
        )

    try:
        task = BackgroundTask(leave_all_groups, user_id_=user_id, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
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
        status_code=status.HTTP_400_BAD_REQUEST, detail=f"{error_code}: {e.message}",
    )
