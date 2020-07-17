import sys
import logging
import socket
from typing import List, Optional

import redis

from datetime import datetime as dt
from datetime import timedelta

from dinofw.cache import ICache
from dinofw.config import ConfigKeys, RedisKeys
from dinofw.db.rdbms.schemas import UserStatsBase

logger = logging.getLogger(__name__)


class MemoryCache:
    def __init__(self):
        self.vals = dict()

    def set(self, key, value, ttl=30):
        try:
            expires_at = (dt.utcnow() + timedelta(seconds=ttl)).timestamp()
            self.vals[key] = (expires_at, value)
        except:
            pass

    def get(self, key):
        try:
            if key not in self.vals:
                return None
            expires_at, value = self.vals[key]
            now = dt.utcnow().timestamp()
            if now > expires_at:
                del self.vals[key]
                return None
            return value
        except:
            return None

    def delete(self, key):
        if key in self.vals:
            del self.vals[key]

    def flushall(self):
        self.vals = dict()


class CacheRedis(ICache):
    def __init__(self, env, host: str, port: int = 6379, db: int = 0):
        if env.config.get(ConfigKeys.TESTING, False) or host == "mock":
            from fakeredis import FakeStrictRedis

            self.redis_pool = None
            self.redis_instance = FakeStrictRedis(host=host, port=port, db=db)
        else:
            self.redis_pool = redis.ConnectionPool(host=host, port=port, db=db)
            self.redis_instance = None

        self.cache = MemoryCache()

        args = sys.argv
        for a in ["--bind", "-b"]:
            bind_arg_pos = [i for i, x in enumerate(args) if x == a]
            if len(bind_arg_pos) > 0:
                bind_arg_pos = bind_arg_pos[0]
                break

        self.listen_port = "standalone"
        if bind_arg_pos is not None and not isinstance(bind_arg_pos, list):
            self.listen_port = args[bind_arg_pos + 1].split(":")[1]

        self.listen_host = socket.gethostname().split(".")[0]

    def get_user_ids_in_group(self, group_id: str):
        return self.redis.smembers(RedisKeys.user_ids_in_group(group_id))

    def set_user_ids_in_group(self, group_id: str, user_ids: List[int]):
        key = RedisKeys.user_ids_in_group(group_id)
        self.redis.delete(key)
        return self.redis.sadd(key, *user_ids)

    def get_user_stats_group(self, group_id: str, user_id: int) -> Optional[dt, dt, dt]:
        """
        :return: [last_read, last_sent, hide_before]
        """
        def to_dt(byte_ts):
            int_ts = int(float(str(byte_ts, "utf-8")))
            return dt.utcfromtimestamp(int_ts)

        key = RedisKeys.user_stats_in_group(group_id)
        user_stats = self.redis.hget(key, user_id)

        if user_stats is None:
            return None, None, None

        return [to_dt(timestamp) for timestamp in user_stats.split("|")]

    def set_user_stats_group(self, group_id: str, user_id: int, stats: UserStatsBase) -> Optional[dt, dt, dt]:
        def to_ts(datetime: dt):
            return datetime.strftime("%s")

        stats_list = [stats.last_read, stats.last_sent, stats.hide_before]
        user_stats = "|".join([to_ts(stat) for stat in stats_list])

        key = RedisKeys.user_stats_in_group(group_id)
        self.redis.hset(key, user_id, user_stats)

    @property
    def redis(self):
        if self.redis_pool is None:
            return self.redis_instance
        return redis.Redis(connection_pool=self.redis_pool)

    def _flushall(self) -> None:
        self.redis.flushdb()
        self.cache.flushall()

    def _set(self, key, val, ttl=None) -> None:
        if ttl is None:
            self.cache.set(key, val)
        else:
            self.cache.set(key, val, ttl=ttl)

    def _get(self, key):
        return self.cache.get(key)

    def _del(self, key) -> None:
        self.cache.delete(key)
