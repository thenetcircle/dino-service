from enum import Enum
from typing import List
from typing import Optional

from pydantic import BaseModel


class AbstractQuery(BaseModel):
    pass


class ActionLogQuery(BaseModel):
    payload: str

    # not all action log creations should update last_message_time and unread count
    update_unread_count: Optional[bool] = False

    # in some cases the 'last message preview' on the group should not be updated
    update_last_message: Optional[bool] = True

    # in some cases the api route doesn't include the user id, e.g.
    # join/kick/etc., but should be recorded on the action log
    user_id: Optional[int]

    # POST /actions api specifies group_id/receiver_id on the query for generic action logs
    group_id: Optional[str]
    receiver_id: Optional[int]


class CreateActionLogQuery(AbstractQuery):
    action_log: Optional[ActionLogQuery]


class PaginationQuery(AbstractQuery):
    until: Optional[float]
    since: Optional[float]
    per_page: int


class AdminQuery(AbstractQuery):
    admin_id: Optional[int]


class NotificationGroup(AbstractQuery):
    user_ids: List[int]
    data: dict


# only used for matching in BroadcastResource, some types need extra info
class EventType:
    message = "message"
    recall = "recall"
    read = "read"
    hide = "hide"
    unhide = "unhide"
    delete = "delete"
    highlight = "highlight"
    delete_attachment = "delete_attachment"


class HighlightStatus:
    RECEIVER = 2
    SENDER = 1
    NONE = 0


class NotificationQuery(AbstractQuery):
    group_id: str
    event_type: str  # previously an Enum, but dino doesn't need to validate it, so changed to string
    notification: List[NotificationGroup]

    class Config:
        use_enum_values = True


class OneToOneQuery(AbstractQuery):
    receiver_id: Optional[int]


class OnlySenderQuery(AbstractQuery):
    only_sender: Optional[bool] = False


class CountMessageQuery(OnlySenderQuery):
    only_attachments: Optional[bool] = False


class MessageQuery(PaginationQuery, AdminQuery, OnlySenderQuery):
    pass


class SendMessageQuery(OneToOneQuery):
    message_payload: Optional[str]
    message_type: int
    context: Optional[str]


class LastReadQuery(AbstractQuery):
    user_id: Optional[int] = None


class CreateGroupQuery(AbstractQuery):
    users: List[int]

    group_name: str
    group_type: int
    description: Optional[str]
    meta: Optional[int]


class UserStatsQuery(AbstractQuery):
    hidden: Optional[bool] = False
    count_unread: Optional[bool] = True
    only_unread: Optional[bool] = True


class GroupInfoQuery(AbstractQuery):
    count_messages: Optional[bool] = False


class ReceiverStatsQuery(AbstractQuery):
    receiver_stats: Optional[bool] = False


class GroupQuery(PaginationQuery, UserStatsQuery, ReceiverStatsQuery):
    receiver_ids: Optional[List[int]]
    group_type: Optional[int] = None


class GroupUpdatesQuery(GroupQuery):
    pass


class JoinGroupQuery(CreateActionLogQuery):
    users: List[int]


class AttachmentQuery(CreateActionLogQuery):
    file_id: str


class DeleteAttachmentQuery(CreateActionLogQuery):
    file_id: Optional[str]
    status: Optional[int]


class UpdateGroupQuery(CreateActionLogQuery):
    status: Optional[int]
    owner: Optional[int]
    group_name: Optional[str]
    description: Optional[str]


class MessageInfoQuery(AbstractQuery):
    group_id: str
    created_at: float  # needed to avoid large table scans


class CreateAttachmentQuery(AttachmentQuery, OneToOneQuery):
    message_payload: str
    created_at: float
    group_id: Optional[str]


class UpdateUserGroupStats(CreateActionLogQuery):
    last_read_time: Optional[float]
    delete_before: Optional[float]
    highlight_time: Optional[float]
    highlight_limit: Optional[int]
    hide: Optional[bool]
    bookmark: Optional[bool]
    pin: Optional[bool]
    rating: Optional[int]


class EditMessageQuery(OneToOneQuery):
    action_log: Optional[ActionLogQuery]

    created_at: float
    group_id: Optional[str]
    context: Optional[str]
    message_payload: Optional[str]
