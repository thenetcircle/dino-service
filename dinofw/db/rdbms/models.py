from datetime import timezone

import sqlalchemy as sa
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from dinofw.environ import env


class UTCDateTime(sa.TypeDecorator):  # pylint:disable=W0223
    impl = sa.DateTime

    def process_bind_param(self, value, dialect):
        if value is not None:
            if not value.tzinfo:
                value = value.replace(tzinfo=timezone.utc)
            return value.astimezone(timezone.utc)

        return None

    def process_result_value(self, value, dialect):
        if value:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return None


class GroupEntity(env.Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), nullable=False, index=True)
    name = Column(String(128), nullable=False)

    status = Column(Integer, nullable=True)
    group_type = Column(Integer, nullable=False, server_default="0")
    created_at = Column(UTCDateTime(), nullable=False)
    owner_id = Column(Integer, nullable=False)

    last_message_time = Column(UTCDateTime(), nullable=False, index=True)
    last_message_id = Column(String(36))
    last_message_overview = Column(String(512))

    updated_at = Column(UTCDateTime())
    group_meta = Column(Integer)
    group_weight = Column(Integer)
    group_context = Column(String(512))
    description = Column(String(256))


class UserGroupStatsEntity(env.Base):
    __tablename__ = "user_group_stats"

    id = Column(Integer, primary_key=True, autoincrement=True)

    group_id = Column(String(36), index=True, nullable=False)
    user_id = Column(Integer, index=True, nullable=False)

    last_read = Column(UTCDateTime(), nullable=False)
    last_sent = Column(UTCDateTime(), nullable=False)
    hide_before = Column(UTCDateTime(), nullable=False)
    delete_before = Column(UTCDateTime(), nullable=False)
    join_time = Column(UTCDateTime(), nullable=False)
