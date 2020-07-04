from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection

from dinofw.rest.models import MessageQuery
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
