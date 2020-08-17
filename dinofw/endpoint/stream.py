from abc import ABC

import redis

from dinofw.config import ConfigKeys


class RequestBuilder:
    @staticmethod
    def parse(message: dict):
        event_type = message["event_type"]
        user_ids = message["user_ids"]
        user_ids = set([int(user_id) for user_id in user_ids.split(",")])

        del message["user_ids"]
        del message["event_type"]

        return user_ids, event_type, message


class IStreamReader(ABC):
    pass


class StreamReader(IStreamReader):
    def __init__(self, env, host: str, port: int = 6379, db: int = 0):
        self.env = env

        if env.config.get(ConfigKeys.TESTING, default=False) or host == "mock":
            from fakeredis import FakeStrictRedis

            self.redis_pool = None
            self.redis_instance = FakeStrictRedis(host=host, port=port, db=db)
        else:
            self.redis_pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.redis_instance = None

        self.consumer_stream = env.config.get(ConfigKeys.STREAM, domain=ConfigKeys.PUBLISHER)
        self.consumer_group = env.config.get(ConfigKeys.GROUP, domain=ConfigKeys.PUBLISHER)

        # TODO: check that we don't recreate stuff unnecessarily with this command
        self.redis.xgroup_create(self.consumer_stream, self.consumer_group, id="$", mkstream=True)

    def consume(self):
        while True:
            self.try_to_consume()

    def try_to_consume(self):
        for stream, messages in self.redis.xreadgroup(
            group_name=self.consumer_group,
            # TODO: each node needs a unique name
            consumer_name="foo",
            streams={
                # TODO: need to keep track of last id each loop?
                self.consumer_stream: "$"  # "$" means last message
            },
            noack=False,
            block=50,  # ms to wait if no new messages
        ):
            for message in messages:
                self.handle_message(message)

    def handle_message(self, message: dict):
        user_ids, event_type, data = RequestBuilder.parse(message)

        for user_id in user_ids:
            self.env.out_of_scope_emit(
                event_type,
                data,
                room=user_id,
                json=True,
                namespace="/ws",
                broadcast=False,
            )

    @property
    def redis(self):
        if self.redis_pool is None:
            return self.redis_instance
        return redis.Redis(connection_pool=self.redis_pool)
