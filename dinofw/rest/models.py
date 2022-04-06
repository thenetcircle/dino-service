from typing import List
from typing import Optional

from pydantic import BaseModel


class GroupJoinTime(BaseModel):
    user_id: int
    join_time: float


class GroupLastRead(BaseModel):
    user_id: int
    last_read: float


class GroupUsers(BaseModel):
    group_id: str
    owner_id: int
    user_count: int
    users: List[GroupJoinTime]


class UserStats(BaseModel):
    user_id: int
    last_update_time: Optional[float]
    last_sent_time: Optional[float]
    last_sent_group_id: Optional[str]

    # total number of unread messages in all 1v1/groups
    unread_amount: int

    # number of 1v1/groups the user has joined
    group_amount: int
    one_to_one_amount: int

    # number of 1v1/groups with at least one unread message
    unread_groups_amount: int


class UserGroupStats(BaseModel):
    group_id: str
    user_id: int
    unread: int
    receiver_unread: int
    delete_before: float
    join_time: float
    last_read_time: Optional[float]
    last_sent_time: Optional[float]
    highlight_time: Optional[float]
    last_updated_time: float
    first_sent: Optional[float]
    attachment_amount: Optional[int] = -1

    hide: Optional[bool]
    pin: Optional[bool]
    deleted: Optional[bool]
    bookmark: Optional[bool]
    rating: Optional[int]

    receiver_highlight_time: Optional[float]
    receiver_delete_before: Optional[float]
    receiver_hide: Optional[bool]
    receiver_deleted: Optional[bool]


class Message(BaseModel):
    group_id: str
    created_at: float
    user_id: int

    message_id: str
    message_type: int
    message_payload: Optional[str]
    context: Optional[str]

    updated_at: Optional[float]

    # don't need to include it in the response, only used for querying
    # file_id: Optional[str]


class Group(BaseModel):
    group_id: str
    users: List[GroupJoinTime]
    user_count: int
    name: str
    description: Optional[str]
    status: Optional[int]
    group_type: int
    created_at: float
    updated_at: Optional[float]
    owner_id: int
    meta: Optional[int]
    first_message_time: float  # TODO: this is probably not needed for the rest api, just internal to track deletions
    last_message_time: float
    last_message_overview: Optional[str]
    last_message_user_id: Optional[int]
    last_message_type: Optional[int]
    last_message_id: Optional[str]
    user_count: int
    message_amount: Optional[int] = -1


class GroupMessage(BaseModel):
    group: Group
    message: Message


class OneToOneStats(BaseModel):
    group: Group
    stats: List[UserGroupStats]


class UserGroup(BaseModel):
    group: Group
    stats: UserGroupStats


class UsersGroup(BaseModel):
    group: Group
    stats: List[UserGroupStats]


class Histories(BaseModel):
    messages: List[Message]
    last_reads: List[GroupLastRead]


class MessageCount(BaseModel):
    group_id: str
    user_id: int
    delete_before: float
    message_count: int
