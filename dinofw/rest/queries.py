from typing import List
from typing import Optional

from pydantic import BaseModel, Field


class AbstractQuery(BaseModel):
    pass


class ActionLogQuery(BaseModel):
    payload: str

    # not all action log creations should update last_message_time and unread count
    update_unread_count: Optional[bool] = False

    # in some cases the 'last message preview' on the group should not be updated
    update_last_message: Optional[bool] = True

    # sometimes, the preview should update, but not the time (i.e. keep ordering of conversations)
    update_last_message_time: Optional[bool] = True

    # for e.g. nickname changes, don't update the group updated_at time, or it will undelete and reorder conversations
    update_group_updated_at: Optional[bool] = True

    unhide_group: Optional[bool] = Field(
        default=True,
        description="""
            When a user changes his/her nickname, an action log is created in all groups, 
            but in this case we don't want to unhide all the groups. Default value is True.
        """
    )

    # in some cases the api route doesn't include the user id, e.g.
    # join/kick/etc., but should be recorded on the action log
    user_id: Optional[int]

    # POST /actions api specifies group_id/receiver_id on the query for generic action logs
    group_id: Optional[str]
    receiver_id: Optional[int]


class SessionUser(AbstractQuery):
    client_id: str
    is_online: bool
    topic: str
    community: str
    user_id: int


class UpdateSessionsQuery(AbstractQuery):
    users: List[SessionUser]


class CreateActionLogQuery(AbstractQuery):
    action_log: Optional[ActionLogQuery]


class PaginationQuery(AbstractQuery):
    until: Optional[float]
    since: Optional[float]
    per_page: int


class AdminQuery(AbstractQuery):
    admin_id: Optional[int]
    include_deleted: Optional[bool] = False


class NotificationGroup(AbstractQuery):
    topic: Optional[str]  # send to specific "group" topic instead of single user topics, e.g. chatops topic
    user_ids: Optional[List[int]]
    data: dict


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


class GetOneToOneQuery(OneToOneQuery):
    only_group_info: Optional[bool] = False


class OnlySenderQuery(AbstractQuery):
    only_sender: Optional[bool] = False


class CountMessageQuery(OnlySenderQuery, AdminQuery):
    only_attachments: Optional[bool] = False


class MessageQuery(PaginationQuery, AdminQuery, OnlySenderQuery):
    pass


class SendMessageQuery(OneToOneQuery):
    message_payload: Optional[str]
    message_type: int
    mention_user_ids: Optional[List[int]] = []
    context: Optional[str]

    # to keep image order same as user chose, and to avoid primary key collision on created_at
    index: Optional[int] = 0


class UserIdQuery(AbstractQuery):
    user_id: Optional[int] = None


class CreateGroupQuery(AbstractQuery):
    users: List[int]

    language: Optional[str]
    group_name: str
    group_type: int
    description: Optional[str]
    meta: Optional[int]


class UserStatsQuery(AbstractQuery):
    hidden: Optional[bool] = None  # None means both
    count_unread: Optional[bool] = True
    only_unread: Optional[bool] = True


class GroupInfoQuery(AbstractQuery):
    count_messages: Optional[bool] = False


class ReceiverStatsQuery(AbstractQuery):
    receiver_stats: Optional[bool] = False


class GroupQuery(PaginationQuery, UserStatsQuery, ReceiverStatsQuery):
    receiver_ids: Optional[List[int]]
    group_type: Optional[int] = None
    include_deleted: Optional[bool] = False


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


class PublicGroupQuery(AdminQuery):
    users: Optional[List[int]] = Field(
        description='List of user ids to get public rooms for. If not provided, will return all public groups.',
        default=None
    )
    include_archived: Optional[bool] = False
    spoken_languages: Optional[List[str]] = Field(
        description='List of ISO 639-1 language codes. E.g. "en" for English, "de" for German, "ja" for Japanese.',
        default=None
    )


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
    notifications: Optional[bool]
    kicked: Optional[bool]


class EditMessageQuery(OneToOneQuery):
    action_log: Optional[ActionLogQuery]

    created_at: float
    group_id: Optional[str]
    context: Optional[str]
    message_payload: Optional[str]


class ExportQuery(PaginationQuery, UserIdQuery):
    pass
