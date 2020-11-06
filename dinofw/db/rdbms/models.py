from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import String

from dinofw.utils.environ import env


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True, unique=True)
    name = Column(String(128))

    owner_id = Column(Integer)
    status = Column(Integer, nullable=True)
    group_type = Column(Integer, server_default="0")
    created_at = Column(DateTime(timezone=True))

    # used by clients to sync changed (new name, user left etc.)
    updated_at = Column(DateTime(timezone=True), index=True)

    # used by clients to sort groups by recent messages
    last_message_time = Column(DateTime(timezone=True), index=True, nullable=False)
    last_message_user_id = Column(Integer, nullable=True)
    last_message_id = Column(String(36), nullable=True)
    last_message_type = Column(Integer, nullable=False, server_default="0")
    last_message_overview = Column(String(512), nullable=True)

    meta = Column(Integer, nullable=True)
    context = Column(String(512), nullable=True)
    description = Column(String(256), nullable=True)


class UserGroupStatsEntity(env.Base):
    __tablename__ = "user_group_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True)
    user_id = Column(Integer, index=True)

    last_read = Column(DateTime(timezone=True))
    last_sent = Column(DateTime(timezone=True))
    delete_before = Column(DateTime(timezone=True))
    join_time = Column(DateTime(timezone=True))
    first_sent = Column(DateTime(timezone=True))

    # used to sync changes to apps
    last_updated_time = Column(DateTime(timezone=True), nullable=False)

    # a user can highlight a 1-to-1 group for ANOTHER user
    highlight_time = Column(DateTime(timezone=True), nullable=False)

    # a user can pin groups he/she wants to keep on top, and will be sorted higher than last_message_time
    pin = Column(Boolean, default=False, nullable=False, index=True)

    # a user can hide a group (will be un-hidden as soon as a new message is sent in this group)
    hide = Column(Boolean, default=False, nullable=False)

    # a user can bookmark a group, which makes it count as "one unread message in this group" (only for this user)
    bookmark = Column(Boolean, default=False, nullable=False)

    # a user can rate conversations
    rating = Column(Integer, nullable=True)
