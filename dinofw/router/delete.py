import sys
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from loguru import logger
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.responses import Response
from starlette.status import HTTP_201_CREATED

from dinofw.rest.models import Message
from dinofw.rest.queries import CreateActionLogQuery
from dinofw.rest.queries import DeleteAttachmentQuery
from dinofw.utils import environ
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes, GroupTypes
from dinofw.utils.decorators import wrap_exception
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.perf import timeit

router = APIRouter()


@router.delete("/groups/{group_id}/user/{user_id}/join", response_model=Optional[Message])
@timeit(logger, "DELETE", "/groups/{group_id}/user/{user_id}/join")
@wrap_exception()
async def leave_group(
    user_id: int, group_id: str, query: CreateActionLogQuery, db: Session = Depends(get_db)
) -> Message:
    """
    Leave a group.

    **Potential error codes in response:** 
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        # TODO: double check that this api will only be called for many-to-many groups
        logs = await environ.env.rest.group.leave_groups([group_id], user_id, query, db)
        if len(logs):
            return logs[0]
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.delete("/users/{user_id}/groups", status_code=HTTP_201_CREATED)
@wrap_exception()
async def delete_all_groups_for_user(
    user_id: int, query: CreateActionLogQuery, db: Session = Depends(get_db)
) -> Response:
    """
    When a user removes his/her profile, make the user leave all groups.

    This API is run asynchronously, and returns a `201 Created` instead of
    `200 OK`.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """

    async def leave_all_groups(user_id_, query_, db_):
        await environ.env.rest.group.delete_all_groups_for_user(user_id_, query_, db_)

    try:
        task = BackgroundTask(leave_all_groups, user_id_=user_id, query_=query, db_=db)
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.delete("/groups/{group_id}/attachment", status_code=HTTP_201_CREATED)
@wrap_exception()
async def delete_attachment_with_file_id(
    group_id: str, query: DeleteAttachmentQuery, db: Session = Depends(get_db)
) -> Response:
    """
    Delete an attachment.

    This API is run asynchronously, and returns a `201 Created` instead of
    `200 OK`.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """

    async def _delete_attachment_with_file_id(group_id_, query_, db_):
        await environ.env.rest.message.delete_attachment(group_id_, query_, db_)

    try:
        task = BackgroundTask(
            _delete_attachment_with_file_id, group_id_=group_id, query_=query, db_=db
        )
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.delete(
    "/groups/{group_id}/user/{user_id}/attachments", response_model=Optional[Message]
)
@wrap_exception()
async def delete_attachments_in_group_for_user(
    group_id: str, user_id: int, query: DeleteAttachmentQuery, db: Session = Depends(get_db)
) -> Message:
    """
    Delete all attachments in this group for this user.

    Returns the action log that is created after the deletions are done.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.delete_attachments_in_group_for_user(
            group_id, user_id, query, db
        )
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.delete("/user/{user_id}/attachments", status_code=HTTP_201_CREATED)
@wrap_exception()
async def delete_attachments_in_all_groups_from_user(
    user_id: int, query: DeleteAttachmentQuery, db: Session = Depends(get_db)
) -> Response:
    """
    Delete all attachments send by this user in all groups.

    This API is run asynchronously, and returns a `201 Created` instead of
    `200 OK`.

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """

    async def _delete_attachments_in_all_groups_from_user(user_id_, query_, db_):
        await environ.env.rest.user.delete_all_user_attachments(user_id_, query_, db_)

    try:
        task = BackgroundTask(
            _delete_attachments_in_all_groups_from_user, user_id_=user_id, query_=query, db_=db
        )
        return Response(background=task, status_code=HTTP_201_CREATED)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
