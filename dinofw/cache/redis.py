import sys
import logging
import socket
from typing import List, Optional, Set, Dict

import redis

from datetime import datetime as dt
from datetime import timedelta

from dinofw.cache import ICache
from dinofw.config import ConfigKeys, RedisKeys
from dinofw.db.rdbms.schemas import UserGroupStatsBase
from dinofw.rest.server.models import AbstractQuery

logger = logging.getLogger(__name__)

FIVE_MINUTES = 60 * 5
ONE_HOUR = 60 * 60


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
        if env.config.get(ConfigKeys.TESTING, default=False) or host == "mock":
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

    def increase_unread_in_group(self, group_id: str):
        key = RedisKeys.unread_in_group(group_id)
        users = self.redis.hgetall(key)

        # TODO: use multiexec
        for user_id, unread in users.items():
            user_id = str(user_id, "utf-8")
            unread = int(str(unread, "utf-8"))
            self.redis.hset(key, user_id, unread + 1)

    def get_last_read_in_group_for_user(self, group_id: str, user_id: int) -> Optional[float]:
        key = RedisKeys.last_read_time(group_id)
        last_read = self.redis.hget(key, user_id)

        if last_read is None:
            return None

        return float(str(last_read, "utf-8"))

    def set_last_read_in_group_for_user(self, group_id: str, user_id: int, last_read: float) -> None:
        key = RedisKeys.last_read_time(group_id)
        self.redis.hset(key, user_id, last_read)

    def get_unread_in_group(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.unread_in_group(group_id)

        n_unread = self.redis.hget(key, user_id)
        if n_unread is None:
            return None

        try:
            return int(str(n_unread, "utf-8"))
        except (TypeError, ValueError):
            return None

    def set_unread_in_group(self, group_id: str, user_id: int, unread: int) -> None:
        key = RedisKeys.unread_in_group(group_id)
        self.redis.hset(key, user_id, unread)

    def get_user_count_in_group(self, group_id: str) -> Optional[int]:
        key = RedisKeys.user_in_group(group_id)
        n_users = self.redis.scard(key)

        if n_users is None:
            return None

        return n_users

    def get_user_ids_and_join_time_in_group(self, group_id: str):
        users = self.redis.smembers(RedisKeys.user_in_group(group_id))
        users = [str(user, "utf-8").split("|") for user in users]

        return {int(user_id): float(join_time) for user_id, join_time in users}

    def set_user_ids_and_join_time_in_group(
        self, group_id: str, users: Dict[int, float]
    ):
        key = RedisKeys.user_in_group(group_id)
        self.redis.delete(key)

        self.add_user_ids_and_join_time_in_group(group_id, users)
        self.redis.expire(key, ONE_HOUR)  # TODO: maybe expire quicker

    def add_user_ids_and_join_time_in_group(self, group_id: str, users: Dict[int, float]) -> None:
        key = RedisKeys.user_in_group(group_id)

        values = [
            "|".join([str(user_id), str(join_time)])
            for user_id, join_time in users.items()
        ]

        self.redis.sadd(key, *values)

    def set_hide_group(self, group_id: str, hide: bool, user_ids: List[int] = None) -> None:
        key = RedisKeys.hide_group(group_id)

        if user_ids is None:
            users = [str(user, "utf-8") for user in self.redis.hgetall(key)]
        else:
            users = user_ids

        for user in users:
            self.redis.hset(key, user, "t" if hide else "f")

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
