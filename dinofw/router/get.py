import sys
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from loguru import logger
from sqlalchemy.orm import Session

from dinofw.rest.models import MessageCount
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserGroupStats
from dinofw.rest.models import UsersGroup
from dinofw.rest.queries import AbstractQuery
from dinofw.rest.queries import GroupInfoQuery
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.decorators import timeit, wrap_exception
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException

router = APIRouter()


@router.get("/groups/{group_id}/users", response_model=Optional[UserGroup])
@timeit(logger, "GET", "/groups/{group_id}/users")
@wrap_exception()
async def get_all_users_statistics_in_group(
    group_id: str, db: Session = Depends(get_db)
) -> UsersGroup:
    """
    Get all users statistics in a group (last read, hidden, etc.), including the group information.

    **Potential error codes in response:**
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        message_amount = await environ.env.rest.group.count_messages_in_group(group_id)
        users_group_stats = await environ.env.rest.group.get_all_user_group_stats(
            group_id, db
        )

        query = GroupInfoQuery(count_messages=False)
        group_info = await environ.env.rest.group.get_group(group_id, query, db, message_amount=message_amount)

        return UsersGroup(group=group_info, stats=users_group_stats)

    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.get("/groups/{group_id}/user/{user_id}", response_model=Optional[UserGroup])
@timeit(logger, "GET", "/groups/{group_id}/user/{user_id}")
@wrap_exception()
async def get_user_statistics_in_group(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> UserGroup:
    """
    Get a user's statistic in a group (last read, hidden, etc.), including the group information.

    **Potential error codes in response:** 
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        message_amount = await environ.env.rest.group.count_messages_in_group(group_id)
        user_group_stats = await environ.env.rest.group.get_user_group_stats(
            group_id, user_id, db
        )

        query = GroupInfoQuery(count_messages=False)
        group_info = await environ.env.rest.group.get_group(group_id, query, db, message_amount=message_amount)

        return UserGroup(group=group_info, stats=user_group_stats)

    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.get("/groups/{group_id}/user/{user_id}/count", response_model=Optional[MessageCount])
@timeit(logger, "GET", "/groups/{group_id}/user/{user_id}/count")
@wrap_exception()
async def get_message_count_for_user_in_group(
    group_id: str, user_id: int, db: Session = Depends(get_db)
) -> MessageCount:
    """
    Count the number of messages in a group since a user's `delete_before`.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        group_info: UserGroupStats = environ.env.db.get_user_stats_in_group(group_id, user_id, db)
        messages_since = environ.env.storage.count_messages_in_group_since(group_id, group_info.delete_before)

        return MessageCount(
            group_id=group_id,
            user_id=user_id,
            delete_before=AbstractQuery.to_ts(group_info.delete_before),
            message_count=messages_since
        )

    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
