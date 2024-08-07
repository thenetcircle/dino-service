from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GroupBase(BaseModel):
    group_id: str
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: Optional[datetime]
    language: Optional[str]

    status: int = 0
    status_changed_at: Optional[datetime]

    first_message_time: datetime
    last_message_time: datetime
    last_message_id: Optional[str]
    last_message_overview: Optional[str]
    last_message_type: Optional[int]
    last_message_user_id: Optional[int]

    group_type: int = 0
    owner_id: Optional[int]
    meta: Optional[int]


class UserGroupStatsBase(BaseModel):
    group_id: str
    user_id: int

    last_read: datetime
    last_sent: datetime
    delete_before: datetime
    join_time: datetime
    highlight_time: Optional[datetime]
    last_updated_time: datetime
    first_sent: Optional[datetime]
    receiver_highlight_time: Optional[datetime]

    sent_message_count: int
    unread_count: int
    deleted: bool
    hide: bool
    pin: bool
    bookmark: bool
    mentions: int
    notifications: bool
    kicked: bool
    rating: Optional[int]


class DeletedStatsBase(BaseModel):
    group_id: str
    user_id: int
    group_type: int

    join_time: datetime
    delete_time: datetime


class Group(GroupBase):
    id: int

    class Config:
        orm_mode = True


class UserGroupStats(UserGroupStatsBase):
    id: int

    class Config:
        orm_mode = True


class UserGroupBase(BaseModel):
    group: GroupBase
    user_stats: UserGroupStatsBase
    receiver_user_stats: Optional[UserGroupStatsBase]
    user_join_times: dict
    user_count: int
    receiver_unread: int
    unread: int
