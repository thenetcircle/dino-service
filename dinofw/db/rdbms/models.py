from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import String
from sqlalchemy import UniqueConstraint

from dinofw.utils.environ import env


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True, unique=True)
    name = Column(String(128))

    owner_id = Column(Integer, nullable=True)
    group_type = Column(Integer, index=True, server_default="0")
    language = Column(String(2), nullable=True)
    created_at = Column(DateTime(timezone=True))

    status = Column(Integer, nullable=False, server_default="0")
    status_changed_at = Column(DateTime(timezone=True), nullable=True)

    # used to schedule batch deletions; if every user in a group has delete_before
    # after this time, we remove messages up to the older delete_before, and set
    # this field to the time of the new first message
    first_message_time = Column(DateTime(timezone=True), index=True, nullable=False)

    # used by clients to sync changes to apps (new name, user left etc.)
    updated_at = Column(DateTime(timezone=True), index=True)

    # used by clients to sort groups by recent messages
    last_message_time = Column(DateTime(timezone=True), index=True, nullable=False)
    last_message_user_id = Column(Integer, nullable=True)
    last_message_id = Column(String(36), nullable=True)
    last_message_type = Column(Integer, nullable=False, server_default="0")
    last_message_overview = Column(Text(), nullable=True)

    meta = Column(Integer, nullable=True)
    description = Column(String(512), nullable=True)

    UniqueConstraint('group_id')


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

    # if a user has been removed from a group by the owner or a moderator, keep in the user's
    # list of conversations but don't let the user get history or other users to see this user
    kicked = Column(Boolean, default=False, nullable=False)

    # increase by one on new message, set to 0 on updating 'last_read'
    unread_count = Column(Integer, nullable=False, server_default="0")

    # increase by one on new message, set to 0 on updating 'delete_before'
    sent_message_count = Column(Integer, nullable=False, server_default="-1")

    # used to sync changes to apps
    last_updated_time = Column(DateTime(timezone=True), nullable=False)

    # a user can highlight a 1-to-1 group for ANOTHER user
    highlight_time = Column(DateTime(timezone=True), nullable=False)

    # for 1-to-1, copy the highlight_time from the receiver
    receiver_highlight_time = Column(DateTime(timezone=True), nullable=False)

    # a user can pin groups he/she wants to keep on top, and will be sorted higher than last_message_time
    pin = Column(Boolean, default=False, nullable=False, index=True)

    # a user can hide a group (will be un-hidden as soon as a new message is sent in this group)
    hide = Column(Boolean, default=False, nullable=False)

    # for syncing group deletions to apps; will become false as soon as a new message is sent (similar to 'hide')
    deleted = Column(Boolean, default=False, nullable=False, server_default="false")

    # a user can bookmark a group, which makes it count as "one unread message in this group" (only for this user)
    bookmark = Column(Boolean, default=False, nullable=False)

    # count the number of mentions in a m2m group; reset when the user opens/reads the group chat
    mentions = Column(Integer, default=0, nullable=False, server_default="0")

    # a user can rate conversations
    rating = Column(Integer, nullable=True)

    # users can disable notifications for a conversation/group, which will disable counting unread messages for it
    notifications = Column(Boolean, default=True, nullable=False)

    UniqueConstraint('group_id', 'user_id')


class DeletedStatsEntity(env.Base):
    __tablename__ = "deleted_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True)
    user_id = Column(Integer, index=True)
    group_type = Column(Integer)

    join_time = Column(DateTime(timezone=True))
    delete_time = Column(DateTime(timezone=True))
