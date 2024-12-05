import sys
from typing import Optional, List

from fastapi import APIRouter
from fastapi import Depends
from loguru import logger
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.responses import Response
from starlette.status import HTTP_201_CREATED

from dinofw.rest.models import Message
from dinofw.rest.queries import JoinGroupQuery, EditMessageQuery, UpdateSessionsQuery
from dinofw.rest.queries import UpdateGroupQuery
from dinofw.rest.queries import UpdateUserGroupStats
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.decorators import wrap_exception
from dinofw.utils.exceptions import NoSuchGroupException, NoSuchMessageException
from dinofw.utils.exceptions import UserIsKickedException
from dinofw.utils.exceptions import UserNotInGroupException
from dinofw.utils.perf import timeit

router = APIRouter()


@router.put("/userstats/{user_id}", status_code=HTTP_201_CREATED)
@timeit(logger, "PUT", "/userstats/{user_id}")
@wrap_exception()
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


@router.put("/mqtt/session", response_model=None)
@timeit(logger, "POST", "/mqtt/session")
@wrap_exception()
async def update_single_mqtt_session(query: UpdateSessionsQuery) -> None:
    """
    INTERNAL API.

    Used by the MQTT bridge to track real-time session updates.
    """
    try:
        return await environ.env.rest.user.update_real_time_user_session(query.users[0])
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/mqtt/sessions", response_model=None)
@timeit(logger, "POST", "/mqtt/sessions")
@wrap_exception()
async def update_batch_mqtt_sessions(query: UpdateSessionsQuery, db: Session = Depends(get_db)) -> None:
    """
    INTERNAL API.

    Used by the MQTT bridge to track offline status in batches.
    """
    try:
        return await environ.env.rest.user.update_user_sessions(query.users, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/user/{user_id}/read", status_code=HTTP_201_CREATED)
@timeit(logger, "PUT", "/user/{user_id}/read")
@wrap_exception()
async def mark_all_groups_as_read(
    user_id: int, db: Session = Depends(get_db)
) -> Response:
    """
    Mark all groups as read, including removing any bookmarks done by the
    user.

    This API is run asynchronously, and returns a 201 Created instead of
    200 OK.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """

    def set_read_time(user_id_, db_):
        environ.env.rest.group.mark_all_as_read(user_id_, db_)

    try:
        task = BackgroundTask(set_read_time, user_id_=user_id, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/groups/{group_id}/user/{user_id}/update")
@timeit(logger, "PUT", "/groups/{group_id}/user/{user_id}/update")
@wrap_exception()
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
    * `highlight_limit`: max number of highlights to allow (will cancel the oldest highlight time if this call causes the highlights to exceed the limit)
    * `hide`: whether to hide/show a conversation,
    * `bookmark`: whether to bookmark a conversation or not,
    * `pin`: whether to pin a conversation or not,
    * `rating`: a user can rate a conversation (1v1 usually),
    * `notifications`: if True, unread count will increase for this user on new messages, if False the unread count will _not_ increase (works for both 1v1 and groups),
    * `kicked`: if True, the user was kicked by the owner or a moderator; the user can't enter the group again, but can see it in the conversation list, without an overview text.

    When setting `bookmark` to false, it will set the unread count to 0,
    and `last_read_time` will be `last_message_time`.

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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/groups/{group_id}", response_model=Message)
@timeit(logger, "PUT", "/groups/{group_id}")
@wrap_exception()
async def edit_group_information(
    group_id, query: UpdateGroupQuery, db: Session = Depends(get_db)
) -> Message:
    """
    Update group details, such as the group name, owner, or whether to archive the group.

    When a group is deleted, it is not removed from the database, but marked as deleted. Users will be kicked out of
    the room and the room will not be visible in the conversation list. A group can be un-deleted, but the group will
    not be restored to its previous state, e.g. the users that were in the group will not be re-added.

    When a group is archived, it is not removed from the database, but marked as archived. Users will not be able to
    send messages to the room, but the room will be visible in the conversation list if `include_archived` is set to
    `True`. Archive is meant to be used for rooms that are not active anymore, but should not be deleted, in case it
    will be brought back at a later time.

    When a group is frozen, it is not removed from the database, but is marked as frozen. Users will not be able to
    send messages to the room, but the room will be visible in the conversation list. A group is normally frozen if a
    review process is in the works, e.g. for a group that is reported for inappropriate content.

    Deleted, archived and frozen are mutually exclusive states, and is decided by the `status` parameter:
    * `0`: normal state,
    * `-1`: frozen,
    * `-2`: archived,
    * `-3`: deleted.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.update_group_information(
            group_id, query, db
        )
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/groups/{group_id}/join", response_model=Optional[Message])
@timeit(logger, "PUT", "/groups/{group_id}/join")
@wrap_exception()
async def join_group(
    group_id: str, query: JoinGroupQuery, db: Session = Depends(get_db)
) -> Optional[Message]:
    """
    Join a group.

    If this is a public/private room, this user will get a `delete_before` set to 1 month ago, or max 500 msgs ago. If
    it's a public/private group, `delete_before` will be the same as the join time (no message history).

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.join_group(group_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/users/{user_id}/message/{message_id}/edit", response_model=Optional[Message])
@timeit(logger, "PUT", "/users/{user_id}/message/{message_id}/edit")
@wrap_exception()
async def edit_message(
    user_id: int, message_id: str, query: EditMessageQuery, db: Session = Depends(get_db)
) -> Message:
    """
    Edit the context or payload of a message. Returns the ActionLog for the edit.

    **Potential error codes in response:**
    * `602`: if the message doesn't exist for the given group and user,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.edit(user_id, message_id, query, db)
    except NoSuchMessageException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_MESSAGE, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
