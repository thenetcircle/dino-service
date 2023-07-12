import sys
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from loguru import logger
from sqlalchemy.orm import Session

from dinofw.rest.models import ClientID, AllDeletedStats, Histories
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UsersGroup
from dinofw.rest.queries import GroupInfoQuery
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.decorators import wrap_exception
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException
from dinofw.utils.perf import timeit

router = APIRouter()


@router.get("/clientid/{domain}/user/{user_id}", response_model=ClientID)
@timeit(logger, "GET", "/clientid/{domain}/user/{user_id}")
@wrap_exception()
async def get_next_available_client_id(domain: str, user_id: int) -> ClientID:
    """
    Get the next available Client ID for a user and domain. Cycles from [0, 49], with
    a TTL of 6h (restart at 0 after 6h).

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    return ClientID(
        client_id=await environ.env.rest.user.get_next_client_id(domain, user_id)
    )


@router.get("/deleted/{user_id}/groups", response_model=AllDeletedStats)
@timeit(logger, "GET", "/deleted/{user_id}/groups")
@wrap_exception()
async def get_deleted_groups_for_user(user_id: int, db: Session = Depends(get_db)) -> AllDeletedStats:
    """
    Get the deletion log of a user.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    return AllDeletedStats(
        stats=await environ.env.rest.user.get_deleted_groups(user_id, db)
    )


@router.get("/history/{group_id}", response_model=Histories)
@timeit(logger, "GET", "/history/{group_id}")
@wrap_exception()
async def get_all_history_in_group(group_id: str) -> Histories:
    """
    Internal api to get all the history in a group for legal purposes.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    return await environ.env.rest.group.all_history_in_group(group_id)


@router.get("/groups/{group_id}/users", response_model=UsersGroup)
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

        # bookmarked groups counts as 1 unread message only if they
        # don't already have unread messages
        if user_group_stats is not None and user_group_stats.unread == 0 and user_group_stats.bookmark:
            user_group_stats.unread = 1

        query = GroupInfoQuery(count_messages=False)
        group_info = await environ.env.rest.group.get_group(group_id, query, db, message_amount=message_amount)

        return UserGroup(group=group_info, stats=user_group_stats)

    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
