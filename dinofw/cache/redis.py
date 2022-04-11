import socket
import sys
from datetime import datetime as dt
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import redis
from loguru import logger

from dinofw.cache import ICache
from dinofw.utils import to_ts, to_dt
from dinofw.utils.config import ConfigKeys
from dinofw.utils.config import RedisKeys

FIVE_MINUTES = 60 * 5
ONE_HOUR = 60 * 60
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY


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

    def get_group_exists(self, group_id: str) -> Optional[bool]:
        value = self.redis.get(RedisKeys.group_exists(group_id))
        if value is None:
            return None
        return str(value, 'utf-8') == '1'

    def set_group_exists(self, group_id: str, exists: bool) -> None:
        self.redis.set(RedisKeys.group_exists(group_id), '1' if exists else '0')

    def get_sent_message_count_in_group_for_user(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.sent_message_count_in_group(group_id)
        count = self.redis.hget(key, str(user_id))

        if count is None:
            return None

        return int(float(str(count, "utf-8")))

    def set_sent_message_count_in_group_for_user(self, group_id: str, user_id: int, count: int) -> None:
        key = RedisKeys.sent_message_count_in_group(group_id)
        self.redis.hset(key, str(user_id), count)

    def get_last_read_in_group_for_users(
        self, group_id: str, user_ids: List[int]
    ) -> Tuple[dict, list]:
        p = self.redis.pipeline()

        for user_id in user_ids:
            key = RedisKeys.last_read_time(group_id)
            p.hget(key, str(user_id))

        not_cached = list()
        last_reads = dict()

        for user_id, last_read in zip(user_ids, p.execute()):
            if last_read is None:
                not_cached.append(user_id)
            else:
                last_reads[user_id] = float(str(last_read, "utf-8"))

        return last_reads, not_cached

    def get_last_read_in_group_for_user(
        self, group_id: str, user_id: int
    ) -> Optional[float]:
        key = RedisKeys.last_read_time(group_id)
        last_read = self.redis.hget(key, str(user_id))

        if last_read is None:
            return None

        return float(str(last_read, "utf-8"))

    def set_last_read_in_group_for_users(
        self, group_id: str, users: Dict[int, float]
    ) -> None:
        key = RedisKeys.last_read_time(group_id)
        p = self.redis.pipeline()

        for user_id, last_read in users.items():
            p.hset(key, str(user_id), last_read)

        p.execute()

    def set_last_read_in_group_for_user(
        self, group_id: str, user_id: int, last_read: float
    ) -> None:
        key = RedisKeys.last_read_time(group_id)
        self.redis.hset(key, str(user_id), last_read)

    def remove_last_read_in_group_for_user(self, group_id: str, user_id: int) -> None:
        key = RedisKeys.last_read_time(group_id)
        self.redis.hdel(key, str(user_id))

    def increase_unread_in_group_for(self, group_id: str, user_ids: List[int]) -> None:
        key = RedisKeys.unread_in_group(group_id)
        p = self.redis.pipeline()

        for user_id in user_ids:
            p.hincrby(key, str(user_id), 1)

        p.execute()

    def reset_unread_in_groups(self, user_id: int, group_ids: List[str]):
        p = self.redis.pipeline()

        for group_id in group_ids:
            key = RedisKeys.unread_in_group(group_id)
            p.hset(key, str(user_id), 0)

        p.execute()

    def get_unread_in_group(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.unread_in_group(group_id)

        n_unread = self.redis.hget(key, str(user_id))
        if n_unread is None:
            return None

        try:
            return int(str(n_unread, "utf-8"))
        except (TypeError, ValueError):
            return None

    def set_unread_in_group(self, group_id: str, user_id: int, unread: int) -> None:
        key = RedisKeys.unread_in_group(group_id)
        self.redis.hset(key, str(user_id), unread)

    def get_user_count_in_group(self, group_id: str) -> Optional[int]:
        key = RedisKeys.user_in_group(group_id)
        n_users = self.redis.hlen(key)

        if n_users is None:
            return None

        return n_users

    def get_messages_in_group(self, group_id: str) -> (Optional[int], Optional[float]):
        key = RedisKeys.messages_in_group(group_id)
        messages_until = self.redis.get(key)

        if messages_until is None:
            return None, None

        messages, until = str(messages_until, "utf-8").split("|")
        return int(messages), float(until)

    def set_last_message_time_in_group(self, group_id: str, last_message_time: float):
        key = RedisKeys.last_message_time(group_id)
        self.redis.set(key, last_message_time)
        self.redis.expire(key, ONE_WEEK)

    def get_last_message_time_in_group(self, group_id: str):
        key = RedisKeys.last_message_time(group_id)
        last_message_time = self.redis.get(key)

        if last_message_time is None:
            return None

        return float(str(last_message_time, "utf-8"))

    def reset_count_group_types_for_user(self, user_id: int) -> None:
        key = RedisKeys.count_group_types_including_hidden(user_id)
        self.redis.delete(key)

        key = RedisKeys.count_group_types_not_including_hidden(user_id)
        self.redis.delete(key)

    def get_delete_before(self, group_id: str, user_id: int) -> Optional[dt]:
        key = RedisKeys.delete_before(group_id, user_id)
        delete_before = self.redis.get(key)

        if delete_before is not None:
            delete_before = float(str(delete_before, "utf-8"))
            return to_dt(delete_before, allow_none=True)

    def set_delete_before(self, group_id: str, user_id: int, delete_before: float) -> None:
        key = RedisKeys.delete_before(group_id, user_id)
        self.redis.set(key, delete_before)
        self.redis.expire(key, 14 * ONE_DAY)

    def increase_attachment_count_in_group_for_users(self, group_id: str, user_ids: List[int]):
        p = self.redis.pipeline()

        # loop-invariant-global-usage
        two_weeks = ONE_DAY * 14

        for user_id in user_ids:
            key = RedisKeys.attachment_count_group_user(group_id, user_id)
            p.incr(key)
            p.expire(key, two_weeks)

        p.execute()

    def remove_attachment_count_in_group_for_users(self, group_id: str, user_ids: List[int]):
        keys = [
            RedisKeys.attachment_count_group_user(group_id, user_id)
            for user_id in user_ids
        ]
        self.redis.delete(*keys)

    def get_attachment_count_in_group_for_user(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.attachment_count_group_user(group_id, user_id)
        the_count = self.redis.get(key)

        if the_count is None:
            return None

        return int(float(str(the_count, "utf-8")))

    def set_attachment_count_in_group_for_user(self, group_id: str, user_id: int, the_count: int) -> None:
        key = RedisKeys.attachment_count_group_user(group_id, user_id)
        self.redis.set(key, str(the_count))
        self.redis.expire(key, ONE_DAY * 14)

    def set_last_sent_for_user(self, user_id: int, group_id: str, last_time: float) -> None:
        key = RedisKeys.last_sent_time_user(user_id)
        self.redis.set(key, f"{group_id}:{last_time}")

    def get_last_sent_for_user(self, user_id: int) -> (str, float):
        key = RedisKeys.last_sent_time_user(user_id)
        values = self.redis.get(key)
        if values is None:
            return None, None

        group_id, last_time = str(values, "utf-8").split(":", maxsplit=1)
        return group_id, float(last_time)

    def set_count_group_types_for_user(self, user_id: int, counts: List[Tuple[int, int]], hidden: bool) -> None:
        if hidden:
            key = RedisKeys.count_group_types_including_hidden(user_id)
        else:
            key = RedisKeys.count_group_types_not_including_hidden(user_id)

        types = ",".join([":".join(map(str, values)) for values in counts])

        self.redis.set(key, types)
        self.redis.expire(key, ONE_DAY)

    def get_count_group_types_for_user(self, user_id: int, hidden: bool) -> Optional[List[Tuple[int, int]]]:
        if hidden is None:
            return None

        if hidden:
            key = RedisKeys.count_group_types_including_hidden(user_id)
        else:
            key = RedisKeys.count_group_types_not_including_hidden(user_id)

        count = self.redis.get(key)
        if count is None:
            return None

        types = str(count, "utf-8")

        # TODO: log and check this, was blank in redis once
        if len(types) == 0 or "," not in types:
            logger.warning(f"group types was none in cache for key {key}")
            return None

        types = types.split(",")
        types = [
            group_type.split(":", maxsplit=1)
            for group_type in types
        ]

        return [(int(a), int(b)) for a, b in types]

    def set_messages_in_group(self, group_id: str, n_messages: int, until: float) -> None:
        key = RedisKeys.messages_in_group(group_id)
        messages_until = f"{n_messages}|{until}"

        self.redis.set(key, messages_until)
        self.redis.expire(key, ONE_HOUR)  # can't cache forever, since users may delete historical messages

    def get_user_ids_and_join_time_in_groups(self, group_ids: List[str]):
        join_times = dict()

        p = self.redis.pipeline()
        for group_id in group_ids:
            p.hgetall(RedisKeys.user_in_group(group_id))

        for group_id, users in zip(group_ids, p.execute()):
            if not len(users):
                continue

            join_times[group_id] = {
                int(user_id): float(join_time)
                for user_id, join_time in users.items()
            }

        return join_times

    def set_user_ids_and_join_time_in_groups(
        self, group_users: Dict[str, Dict[int, float]]
    ):
        p = self.redis.pipeline()

        for group_id, users in group_users.items():
            key = RedisKeys.user_in_group(group_id)
            p.delete(key)

            if len(users):
                for user_id, join_time in users.items():
                    p.hset(key, str(user_id), str(join_time))
                p.expire(key, ONE_DAY)

        p.execute()

    def get_user_ids_and_join_time_in_group(
        self, group_id: str
    ) -> Optional[Dict[int, float]]:
        users = self.redis.hgetall(RedisKeys.user_in_group(group_id))

        if not len(users):
            return None

        return {int(user_id): float(join_time) for user_id, join_time in users.items()}

    def set_user_ids_and_join_time_in_group(
        self, group_id: str, users: Dict[int, float]
    ):
        key = RedisKeys.user_in_group(group_id)
        self.redis.delete(key)

        if len(users):
            self.add_user_ids_and_join_time_in_group(group_id, users)
            self.redis.expire(key, ONE_HOUR)

    def add_user_ids_and_join_time_in_group(
        self, group_id: str, users: Dict[int, float]
    ) -> None:
        key = RedisKeys.user_in_group(group_id)
        p = self.redis.pipeline()

        for user_id, join_time in users.items():
            p.hset(key, str(user_id), str(join_time))

        p.expire(key, ONE_DAY)
        p.execute()

    def clear_user_ids_and_join_time_in_group(self, group_id: str) -> None:
        key = RedisKeys.user_in_group(group_id)
        self.redis.delete(key)

    def set_hide_group(
        self, group_id: str, hide: bool, user_ids: List[int] = None
    ) -> None:
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
