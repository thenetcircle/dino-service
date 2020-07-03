from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from datetime import datetime as dt
import pytz

from dinofw.rest.models import GroupQuery
from dinofw.storage.models import GroupModel
from dinofw.storage.models import MessageModel


class CassandraHandler:
    def __init__(self):
        connection.setup(
            ['127.0.0.1'],
            default_keyspace="dinofw",
            protocol_version=3,
            retry_connection=True
        )

        sync_table(GroupModel)
        sync_table(MessageModel)

    def get_groups_for_user(self, user_id: int, query: GroupQuery):
        if query.since is None:
            since = dt.utcnow()
            since = since.replace(pytz.UTC)
        else:
            since = dt.strptime(str(query.since), "%s")

        return GroupModel.objects(
            GroupModel.user_id == user_id,
            GroupModel.updated_at <= since
        ).limit(
            query.per_page or 100
        ).all()
