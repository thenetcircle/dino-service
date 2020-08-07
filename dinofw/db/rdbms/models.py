from sqlalchemy import Column, Boolean, DateTime
from sqlalchemy import Integer
from sqlalchemy import String

from dinofw.environ import env


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True)
    name = Column(String(128))

    status = Column(Integer, nullable=True)
    group_type = Column(Integer, server_default="0")
    created_at = Column(DateTime(timezone=True))
    owner_id = Column(Integer)

    # used by clients to sync changed (new name, user left etc.)
    updated_at = Column(DateTime(timezone=True), index=True)

    # users by clients to sort groups by recent messages
    last_message_time = Column(DateTime(timezone=True), index=True)
    last_message_id = Column(String(36))
    last_message_overview = Column(String(512))

    group_meta = Column(Integer)
    group_weight = Column(Integer)
    group_context = Column(String(512))
    description = Column(String(256))


class UserGroupStatsEntity(env.Base):
    __tablename__ = "user_group_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True)
    user_id = Column(Integer, index=True)

    last_read = Column(DateTime(timezone=True))
    last_sent = Column(DateTime(timezone=True))
    delete_before = Column(DateTime(timezone=True))
    join_time = Column(DateTime(timezone=True))

    # a user can highlight a 1-to-1 group for ANOTHER user
    highlight_time = Column(DateTime(timezone=True), nullable=True)

    # a user can hide a group (will be un-hidden as soon as a new message is sent in this group)
    hide = Column(Boolean, default=False)

    # a user can pin groups he/she wants to keep on top, and will be sorted higher than last_message_time
    pin = Column(Boolean, default=False, index=True)

    # a user can bookmark a group, which makes it count as "one unread message in this group" (only for this user)
    bookmark = Column(Boolean, default=False)
