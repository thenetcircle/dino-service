import logging
import sys

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.orm import Session

from dinofw.rest.models import GroupUsers
from dinofw.rest.models import UserGroupStats
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import UserNotInGroupException

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/groups/{group_id}/user/{user_id}", response_model=UserGroupStats)
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
        return await environ.env.rest.group.get_user_group_stats(
            group_id, user_id, message_amount, db
        )
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
