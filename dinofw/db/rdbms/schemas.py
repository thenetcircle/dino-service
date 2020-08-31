from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class GroupBase(BaseModel):
    group_id: str
    name: str
    description: Optional[str]
    created_at: datetime
    updated_at: datetime

    last_message_time: datetime
    last_message_overview: Optional[str]
    last_message_type: Optional[int]

    status: Optional[int]
    group_type: int
    owner_id: int

    meta: Optional[int]
    weight: Optional[int]
    context: Optional[str]


class UserGroupStatsBase(BaseModel):
    group_id: str
    user_id: int

    last_read: datetime
    last_sent: datetime
    delete_before: datetime
    join_time: datetime
    highlight_time: Optional[datetime]
    last_updated_time: datetime

    hide: bool
    pin: bool
    bookmark: bool
    rating: Optional[int]


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
    user_join_times: dict
    user_count: int
    unread_count: int
