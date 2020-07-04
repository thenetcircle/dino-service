import pytz
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection

from uuid import uuid4 as uuid
from datetime import datetime as dt
from dinofw.rest.models import MessageQuery, SendMessageQuery
from dinofw.storage.cassandra_models import MessageModel


class CassandraHandler:
    def __init__(self):
        connection.setup(
            ['maggie-cassandra-1'],
            default_keyspace="dinofw",
            protocol_version=3,
            retry_connect=True
        )

        sync_table(MessageModel)

    def get_messages_in_group(self, group_id: str, query: MessageQuery):
        return MessageModel.objects(
            MessageModel.group_id == group_id,
            MessageModel.created_at <= MessageQuery.to_dt(query.since)
        ).limit(
            query.per_page or 100
        ).all()

    def store_message(self, group_id: str, user_id: int, query: SendMessageQuery):
        created_at = dt.utcnow()
        created_at = created_at.replace(tzinfo=pytz.UTC)
        message_id = uuid()

        MessageModel.create(
            group_id=group_id,
            user_id=user_id,
            created_at=created_at,
            message_id=message_id,
            message_payload=query.message_payload,
            message_type=query.message_type
        )

        return str(message_id)
