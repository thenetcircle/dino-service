import logging
import sys

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.responses import Response
from starlette.status import HTTP_201_CREATED

from dinofw.rest.models import Group
from dinofw.rest.models import UpdateGroupQuery
from dinofw.rest.models import UpdateUserGroupStats
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.decorators import timeit
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException

logger = logging.getLogger(__name__)
router = APIRouter()


@router.put("/userstats/{user_id}", status_code=HTTP_201_CREATED)
@timeit(logger, "PUT", "/userstats/{user_id}")
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


@router.put("/user/{user_id}/read", status_code=HTTP_201_CREATED)
@timeit(logger, "PUT", "/user/{user_id}/read")
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


@router.put("/groups/{group_id}/user/{user_id}/update")
@timeit(logger, "PUT", "/groups/{group_id}/user/{user_id}/update")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/groups/{group_id}")
@timeit(logger, "PUT", "/groups/{group_id}")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.put("/groups/{group_id}/user/{user_id}/join")
@timeit(logger, "PUT", "/groups/{group_id}/user/{user_id}/join")
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
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
