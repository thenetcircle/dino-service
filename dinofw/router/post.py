import sys
from datetime import timedelta
from typing import List
from typing import Optional

from fastapi import APIRouter
from fastapi import Depends
from loguru import logger
from sqlalchemy.orm import Session
from starlette.background import BackgroundTask
from starlette.responses import Response
from starlette.status import HTTP_201_CREATED

from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.models import Group, LastReads
from dinofw.rest.models import Histories
from dinofw.rest.models import Message
from dinofw.rest.models import MessageCount
from dinofw.rest.models import OneToOneStats
from dinofw.rest.models import UserGroup
from dinofw.rest.models import UserStats
from dinofw.rest.queries import ActionLogQuery, LastReadQuery
from dinofw.rest.queries import AttachmentQuery
from dinofw.rest.queries import CountMessageQuery
from dinofw.rest.queries import CreateAttachmentQuery
from dinofw.rest.queries import CreateGroupQuery
from dinofw.rest.queries import GroupInfoQuery
from dinofw.rest.queries import GroupQuery
from dinofw.rest.queries import GroupUpdatesQuery
from dinofw.rest.queries import MessageInfoQuery
from dinofw.rest.queries import MessageQuery
from dinofw.rest.queries import NotificationQuery
from dinofw.rest.queries import OneToOneQuery
from dinofw.rest.queries import SendMessageQuery
from dinofw.rest.queries import UserStatsQuery
from dinofw.utils import environ, is_non_zero, one_year_ago
from dinofw.utils import to_ts
from dinofw.utils.api import get_db
from dinofw.utils.api import log_error_and_raise_known
from dinofw.utils.api import log_error_and_raise_unknown
from dinofw.utils.config import ErrorCodes
from dinofw.utils.decorators import wrap_exception
from dinofw.utils.exceptions import GroupIsFrozenException
from dinofw.utils.exceptions import InvalidRangeException
from dinofw.utils.exceptions import NoSuchAttachmentException
from dinofw.utils.exceptions import NoSuchGroupException
from dinofw.utils.exceptions import NoSuchMessageException
from dinofw.utils.exceptions import NoSuchUserException
from dinofw.utils.exceptions import QueryValidationError
from dinofw.utils.exceptions import UserIsKickedException
from dinofw.utils.exceptions import UserNotInGroupException
from dinofw.utils.perf import timeit

router = APIRouter()


@router.post("/notification/send", response_model=None)
@timeit(logger, "POST", "/notification/send")
@wrap_exception()
async def notify_users(query: NotificationQuery, db: Session = Depends(get_db)) -> None:
    try:
        return await environ.env.rest.broadcast.broadcast_event(query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/send", response_model=Message)
@timeit(logger, "POST", "/users/{user_id}/send")
@wrap_exception()
async def send_message_to_user(
        user_id: int, query: SendMessageQuery, db: Session = Depends(get_db)
) -> Message:
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
    * `607`: group is frozen and no message can be sent,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.send_message_to_user(user_id, query, db)
    except NoSuchUserException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_USER, sys.exc_info(), e)
    except GroupIsFrozenException as e:
        log_error_and_raise_known(ErrorCodes.GROUP_IS_FROZEN, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/message/{message_id}/info", response_model=Optional[Message])
@timeit(logger, "POST", "/users/{user_id}/message/{message_id}/info")
@wrap_exception()
async def get_message_info(
        user_id: int, message_id: str, query: MessageInfoQuery
) -> Message:
    """
    Get details about a message. The `created_at` field on the query is
    needed to avoid large table scans when trying to find the message
    in Cassandra.

    **Potential error codes in response:**
    * `602`: if the message doesn't exist for the given group and user,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.message.get_message_info(user_id, message_id, query)
    except NoSuchMessageException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_MESSAGE, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/groups/{group_id}/user/{user_id}/histories", response_model=Optional[Histories])
@timeit(logger, "POST", "/groups/{group_id}/user/{user_id}/histories")
@wrap_exception()
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

    Only one of `since` and `until` can be used at the same time. At least one needs to be
    specified.

    If `only_sender=true` (default is `false`), the API will only return messages history in
    this group that were sent by `user_id`. This can be combined with `until` and `per_page`,
    to paginate through all the messages for a user, but setting the next query's `until` to
    the `created_at` time of the last message returned from the previous query.

    If `admin_id` is set, and is greater than `0`, the read status will not be updated. Useful
    for getting history in admin UI without updating `last_read_time` of the user.

    If `admin_id>0` and `include_deleted=true`, the result will also include messages that have been deleted by the
    users (up to max one year ago before deletion date). Default value is `false`. Useful for the admin UI. Can be
    combined with `only_sender=true`.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `605`: if the since/until parameters are not valid,
    * `606`: if the user has been kicked from this group he/she is not allowed to get the history,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.group.histories(group_id, user_id, query, db)
    except UserIsKickedException as e:
        log_error_and_raise_known(ErrorCodes.USER_IS_KICKED, sys.exc_info(), e)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except InvalidRangeException as e:
        log_error_and_raise_known(ErrorCodes.WRONG_PARAMETERS, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/groups", response_model=Optional[List[UserGroup]])
@timeit(logger, "POST", "/users/{user_id}/groups")
@wrap_exception()
async def get_groups_for_user(
        user_id: int, query: GroupQuery, db: Session = Depends(get_db)
) -> List[UserGroup]:
    """
    Get a list of groups for this user, sorted by last message sent. For paying users,
    the `count_unread` field can be set to True (default is False).

    If `count_unread` is False, the field `unread` will have the value `-1`, and
    similarly if `receiver_unread` is False, the field `receiver_unread` will have
    the value `-1`.

    If `hidden` is set to True in the query, only hidden groups will be returned.
    If False, only visible groups are returned. If not specified or set to null,
    both hidden and visible groups are returned.

    If `receiver_stats` is True, the following fields will be set:

    * `receiver_unread`
    * `receiver_delete_before`
    * `receiver_hide`
    * `receiver_deleted`

    The `receiver_stats` field is False by default, since not all queries needs this
    information. If it's False, the above fields will be `null`, except for
    `receiver_unread`, which will be -1.

    One receiver stat that is always returned (even if `receiver_stats` is False),
    is `receiver_highlight_time`. Default value is 789000000.0, (means "long ago",
    translates to 1995-01-01 22:40:00 UTC).

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_groups_for_user(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/groups/updates", response_model=Optional[List[UserGroup]])
@timeit(logger, "POST", "/users/{user_id}/groups/updates")
@wrap_exception()
async def get_groups_updated_since(
        user_id: int, query: GroupUpdatesQuery, db: Session = Depends(get_db)
) -> List[UserGroup]:
    """
    Get a list of groups for this user that has changed since a certain time, sorted
    by last message sent. Used to sync changes to mobile apps.

    If `count_unread` is False, the field `unread` will have the value `-1`, and
    similarly if `receiver_unread` is False, the field `receiver_unread` will have
    the value `-1`.

    If `receiver_stats` is True, the following fields will be set:

    * `receiver_unread`
    * `receiver_delete_before`
    * `receiver_hide`
    * `receiver_deleted`

    The `receiver_stats` field is False by default, since not all queries needs this
    information. If it's False, the above fields will be `null`, except for
    `receiver_unread`, which will be -1.

    One receiver stat that is always returned (even if `receiver_stats` is False),
    is `receiver_highlight_time`. Default value is 789000000.0, (means "long ago",
    translates to 1995-01-01 22:40:00 UTC).

    **Potential error codes in response:**
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_groups_updated_since(user_id, query, db)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/userstats/{user_id}", response_model=Optional[UserStats])
@timeit(logger, "POST", "/userstats/{user_id}")
@wrap_exception()
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


@router.post("/groups/{group_id}", response_model=Optional[Group])
@timeit(logger, "POST", "/groups/{group_id}")
@wrap_exception()
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
@wrap_exception()
async def send_message_to_group(
        group_id: str, user_id: int, query: SendMessageQuery, db: Session = Depends(get_db)
) -> Message:
    """
    User sends a message in a group. This API should also be used for **1-to-1** conversations
    if the client knows the `group_id` for the **1-to-1** conversations. Otherwise the
    `POST /v1/users/{user_id}/send` API can be used to send a message and get the `group_id`.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `607`: group is frozen and no message can be sent,
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
    except GroupIsFrozenException as e:
        log_error_and_raise_known(ErrorCodes.GROUP_IS_FROZEN, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/users/{user_id}/group", response_model=Optional[OneToOneStats])
@timeit(logger, "POST", "/users/{user_id}/group")
@wrap_exception()
async def get_one_to_one_information(
        user_id: int, query: OneToOneQuery, db: Session = Depends(get_db)
) -> OneToOneStats:
    """
    Get details about a 1v1 group.

    * `message_amount` is NOT per user, it's the total amount of messages since the creation of the group,
    * `attachment_amount` IS per user, counted since the user's `delete_before`.

    If `admin_id>0` and `include_deleted=true`, message amount will also count messages that have been deleted by the
    users (up to max one year ago before deletion date). Default value is `false`. Useful for the admin UI.

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


@router.post("/users/{user_id}/message/{message_id}/attachment", response_model=Optional[Message])
@timeit(logger, "POST", "/users/{user_id}/message/{message_id}/attachment")
@wrap_exception()
async def create_an_attachment(
        user_id: int,
        message_id: str,
        query: CreateAttachmentQuery,
        db: Session = Depends(get_db),
) -> Message:
    """
    Create an attachment.

    When a user sends an image or video, first call the "send message API" with the
    `message_type` set to `image` (or similar). When the image has finished processing
    in the backend, call this API to create the actual attachment metadata for it.

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


@router.post("/groups/{group_id}/attachment", response_model=Optional[Message])
@timeit(logger, "POST", "/groups/{group_id}/attachment")
@wrap_exception()
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
    "/groups/{group_id}/user/{user_id}/attachments", response_model=Optional[List[Message]]
)
@timeit(logger, "POST", "/groups/{group_id}/user/{user_id}/attachments")
@wrap_exception()
async def get_attachments_in_group_for_user(
        group_id: str, user_id: int, query: MessageQuery, db: Session = Depends(get_db)
) -> List[Message]:
    """
    Get all attachments in this group for this user.

    Only one of `since` and `until` can be used at the same time. At least one needs to be
    specified.

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


@router.post("/users/{user_id}/actions", response_model=Optional[Message])
@timeit(logger, "POST", "/users/{user_id}/actions")
@wrap_exception()
async def create_action_log(
        user_id: int, query: ActionLogQuery, db: Session = Depends(get_db)
) -> None:
    """
    Create an action log in a group.

    If `receiver_id` is specified, the 1-to-1 group will be
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


@router.post("/users/{user_id}/groups/actions", response_model=Optional[Message])
@timeit(logger, "POST", "/users/{user_id}/groups/actions")
@wrap_exception()
async def create_action_log_in_all_groups_for_user(
        user_id: int, query: ActionLogQuery, db: Session = Depends(get_db)
) -> Response:
    """
    Create an action log in all groups this user has joined.

    Only the `payload` field in the request body will be used by this API,
    any other fields that are specified will be ignored.

    The action log parameter `unhide_group` can be set to False. This is
    useful when a user is changing his/her nickname, otherwise all groups
    for this user will be unhidden. Default value is True.

    This API is run asynchronously, and returns a `201 Created` instead of
    `200 OK`.

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


@router.post("/users/{user_id}/groups/create", response_model=Optional[Group])
@timeit(logger, "POST", "/users/{user_id}/groups/create")
@wrap_exception()
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


@router.post("/groups/{group_id}/user/{user_id}/count", response_model=Optional[MessageCount])
@timeit(logger, "POST", "/groups/{group_id}/user/{user_id}/count")
@wrap_exception()
async def get_message_count_for_user_in_group(
        group_id: str, user_id: int, query: Optional[CountMessageQuery] = None, db: Session = Depends(get_db)
) -> MessageCount:
    """
    Count the number of messages in a group since a user's `delete_before`.

    If `only_attachments` is True (default is False), only attachments are
    counted and not all messages. This parameter can NOT be used together
    with `only_sender=true`.

    If `only_sender` is set to False (default value), the messages for all
    users in the groups will be counted. If set to True, only messages sent
    by the specified `user_id` will be counted. When set to True, only
    messages send by this user _after_ his/her `delete_before` and _before_
    his/her `last_sent_time` will be counted.

    If `admin_id>0` and `include_deleted=true`, message amount will also count messages that have been deleted by the
    users (up to max one year ago before deletion date). Default value is `false`. Useful for the admin UI. Can be
    combined with `only_sender=true` and/or `only_attachments=true`.

    Note: setting `only_sender=true` is slow. Around 2 seconds for a group
    of 6k messages. This is because we can not filter by `user_id` in
    Cassandra, and have to instead batch query for all messages in the group
    and filter out and count afterward.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """

    def count_messages():
        # can't filter by user id in cassandra without restricting 'created_at', so
        # use the cached value from the rdbms
        if query and query.only_sender:
            group_info: UserGroupStatsBase = environ.env.db.get_user_stats_in_group(group_id, user_id, db)

            # until isn't inclusive, so the last message sent won't be counted otherwise;
            until = group_info.last_sent
            until += timedelta(seconds=1)
            since = group_info.delete_before

            if is_non_zero(query.admin_id) and query.include_deleted:
                message_count = None
            else:
                # can return both None and -1; -1 means we've checked the db before, but it has not
                # yet been counted, to avoid checking the db every time a new message is sent
                message_count = environ.env.db.get_sent_message_count(group_id, user_id, db)

            # if it hasn't been counted before, count from cassandra in batches (could be slow)
            if message_count is None or message_count == -1:
                message_count = environ.env.storage.count_messages_in_group_from_user_since(
                    group_id,
                    user_id,
                    until=until,
                    since=since,
                    query=query
                )

                # don't cache counts when including deleted messages, it's only used by admins
                if not query.include_deleted:
                    environ.env.db.set_sent_message_count(group_id, user_id, message_count, db)

        else:
            message_count = environ.env.storage.count_messages_in_group_since(
                group_id, delete_before, query
            )

        return message_count

    async def count_attachments():
        return await environ.env.rest.group.count_attachments_in_group_for_user(
            group_id, user_id, delete_before, query
        )

    try:
        delete_before = environ.env.db.get_delete_before(group_id, user_id, db)

        if query.only_attachments:
            the_count = await count_attachments()
        else:
            the_count = count_messages()

        return MessageCount(
            group_id=group_id,
            user_id=user_id,
            delete_before=to_ts(delete_before),
            message_count=the_count
        )

    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)


@router.post("/groups/{group_id}/lastread", response_model=Optional[LastReads])
@timeit(logger, "GET", "/groups/{group_id}/lastread")
@wrap_exception()
async def get_last_read_in_group(
        group_id: str, query: Optional[LastReadQuery] = None, db: Session = Depends(get_db)
) -> LastReads:
    """
    Get the `last_read_time` for either one user in a group, or for all users in a group.

    If no `user_id` is specified in the request body, then the `last_read_time` for ALL
    users will be returned.

    **Potential error codes in response:**
    * `600`: if the user is not in the group,
    * `601`: if the group does not exist,
    * `250`: if an unknown error occurred.
    """
    try:
        return await environ.env.rest.user.get_last_read(group_id, query, db)
    except NoSuchGroupException as e:
        log_error_and_raise_known(ErrorCodes.NO_SUCH_GROUP, sys.exc_info(), e)
    except UserNotInGroupException as e:
        log_error_and_raise_known(ErrorCodes.USER_NOT_IN_GROUP, sys.exc_info(), e)
    except Exception as e:
        log_error_and_raise_unknown(sys.exc_info(), e)
