import logging
import sys
from typing import List

from fastapi import APIRouter
from fastapi import Depends
from starlette.responses import Response
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.status import HTTP_201_CREATED

from dinofw.rest.models import AttachmentQuery
from dinofw.rest.models import ActionLogQuery
from dinofw.rest.models import CreateAttachmentQuery
from dinofw.rest.models import CreateGroupQuery
from dinofw.rest.models import Group
from dinofw.rest.models import GroupInfoQuery
from dinofw.rest.models import GroupQuery
from dinofw.rest.models import GroupUpdatesQuery
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import MessageQuery
from dinofw.rest.models import OneToOneQuery
from dinofw.rest.models import OneToOneStats
from dinofw.rest.models import SendMessageQuery
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserStats
from dinofw.rest.models import UserStatsQuery
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.decorators import timeit
from dinofw.utils.exceptions import NoSuchAttachmentException, QueryValidationError
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import NoSuchMessageException
from dinofw.utils.exceptions import NoSuchUserException
from dinofw.utils.exceptions import UserNotInGroupException

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/users/{user_id}/send", response_model=Message)
@timeit(logger, "POST", "/users/{user_id}/send")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_USER, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/groups/{group_id}/user/{user_id}/histories", response_model=Histories)
@timeit(logger, "POST", "/groups/{group_id}/user/{user_id}/histories")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/groups", response_model=List[UserGroup])
@timeit(logger, "POST", "/users/{user_id}/groups")
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


@router.post("/users/{user_id}/groups/updates", response_model=List[UserGroup])
@timeit(logger, "POST", "/users/{user_id}/groups/updates")
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


@router.post("/userstats/{user_id}", response_model=UserStats)
@timeit(logger, "POST", "/userstats/{user_id}")
async def get_user_statistics(
    user_id: int, query: UserStatsQuery, db: Session = Depends(get_db)
) -> UserStats:
    """
    Get a user's statistics globally (not only for one group).

    Request body can specify `hidden` (default will count both
    hidden and not hidden), `only_unread` (default True), and
    `count_unread` (default True).

    If `hidden=true`, the `one_to_one_amount` and `group_amount`
    fields will ONLY include hidden groups. If `hidden=false` the
    fields will only include NOT HIDDEN groups. If not specified,
    both will be included.

    If `count_unread=false` the flags `unread_amount` and
    `unread_groups_amount` will both be `-1`. Default value is
    `true`.

    If `only_unread=true`, only groups with unread messages will
    be counted. Defaults to `true`. The fields `group_amount` and
    `one_to_one_amount` will be `-1`. Defaults to `true`.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_user_stats(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/groups/{group_id}", response_model=Group)
@timeit(logger, "POST", "/groups/{group_id}")
async def get_group_information(
    group_id: str, query: GroupInfoQuery, db: Session = Depends(get_db)
) -> Group:
    """
    Get details about one group.

    If `count_messages` is set to `true`, a count of all messages in the group
    will be returned in `message_amount`. If `count_messages` is set to `false`,
    `message_amount` will be `-1`. Default value is `false`.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.get_group(group_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/groups/{group_id}/user/{user_id}/send", response_model=Message)
@timeit(logger, "POST", "/groups/{group_id}/user/{user_id}/send")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/group", response_model=OneToOneStats)
@timeit(logger, "POST", "/users/{user_id}/group")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/message/{message_id}/attachment")
@timeit(logger, "POST", "/users/{user_id}/message/{message_id}/attachment")
async def create_an_attachment(
    user_id: int,
    message_id: str,
    query: CreateAttachmentQuery,
    db: Session = Depends(get_db),
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
        return await environ.env.rest.message.create_attachment(
            user_id, message_id, query, db
        )
    except QueryValidationError as e:
        log_error_and_raise_known(ErrorCodes.WRONG_PARAMETERS, sys.exc_info(), e)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except NoSuchMessageException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_MESSAGE, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/groups/{group_id}/attachment", response_model=Message)
@timeit(logger, "POST", "/groups/{group_id}/attachment")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except NoSuchAttachmentException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_ATTACHMENT, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post(
    "/groups/{group_id}/user/{user_id}/attachments", response_model=List[Message]
)
@timeit(logger, "POST", "/groups/{group_id}/user/{user_id}/attachments")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/actions", response_model=Message)
@timeit(logger, "POST", "/users/{user_id}/actions")
async def create_action_log(
    user_id: int, query: ActionLogQuery, db: Session = Depends(get_db)
) -> None:
    """
    Create one or more action logs in group.

    If `query.action_log.receiver_id` is specified, the 1-to-1 group will be
    automatically created if it doesn't already exist. One case when this is
    desirable is when user A sends a friend request to used B; the action log
    is "user A requested to be a friend of user B", but the group needs to
    be created first, and to avoid doing two API calls, the group is
    automatically created.

    Multi-user groups are NOT automatically created when this API is called.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return environ.env.rest.group.create_action_log(query, db, user_id=user_id)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/groups/actions", response_model=Message)
@timeit(logger, "POST", "/users/{user_id}/groups/actions")
async def create_action_log_in_all_groups_for_user(
    user_id: int, query: ActionLogQuery, db: Session = Depends(get_db)
) -> Response:
    """
    Create one or more action logs in all groups this user has joined.

    Only the `payload` field in the request body will be used by this API,
    any other fields that are specified will be ignored.

    This API is run asynchronously, and returns a 201 Created instead of
    200 OK.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """

    def _create_action_logs(user_id_, query_, db_):
        environ.env.rest.user.create_action_log_in_all_groups(user_id_, query_, db_)

    try:
        task = BackgroundTask(
            _create_action_logs, user_id_=user_id, query_=query, db_=db
        )
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/groups/create", response_model=Group)
@timeit(logger, "POST", "/users/{user_id}/groups/create")
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
