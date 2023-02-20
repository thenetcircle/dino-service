import socket
import sys
from contextlib import contextmanager
from datetime import datetime as dt
from datetime import timedelta
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import redis
from loguru import logger

from dinofw.cache import ICache
from dinofw.utils import to_dt
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
            self.redis_instance = FakeStrictRedis(host=host, port=port, db=db, decode_responses=True)

            # fakeredis doesn't use execute on pipelines...
            self.redis_instance.execute = lambda: None

            self.testing = True
        else:
            self.redis_pool = redis.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
            self.redis_instance = None
            self.testing = False

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

    @contextmanager
    def pipeline(self):
        """
        to chain separate cache functions using a single pipeline, will be executed
        at the end of the context managers lifecycle

        :return: a Redis pipeline object
        """
        p = self.redis.pipeline()
        try:
            yield p
        finally:
            if p is not None:
                p.execute()

    def get_next_client_id(self, domain: str, user_id: int) -> str:
        key = RedisKeys.client_id(domain, user_id)
        current_idx = self.redis.llen(key)

        # start from 0 if no pool found
        if current_idx == 0:
            current_idx = -1

        # if we've reached the max 100 ids, reset the pool and restart from 0
        elif current_idx >= 100:
            current_idx = -1
            self.redis.delete(key)

        next_client_id = f"{user_id}_{domain}_{current_idx+1}"
        self.redis.lpush(key, next_client_id)

        # try to reuse lower ids by expiring the pool
        self.redis.expire(key, 6 * ONE_HOUR)

        return next_client_id

    def get_total_unread_count(self, user_id: int) -> (Optional[int], Optional[int]):
        p = self.redis.pipeline()

        p.get(RedisKeys.total_unread_count(user_id))
        p.scard(RedisKeys.unread_groups(user_id))

        try:
            unread_count, unread_groups = p.execute()
        except redis.exceptions.ResponseError:
            # if no value is cached, SCARD will throw an error
            return None, None

        if unread_count is not None:
            return int(float(unread_count)), int(float(unread_groups))

        return None, None

    def set_total_unread_count(self, user_id: int, unread_count: int, unread_groups: List[str]) -> None:
        p = self.redis.pipeline()

        key_count = RedisKeys.total_unread_count(user_id)
        p.set(key_count, unread_count)
        p.expire(key_count, ONE_DAY * 2)

        if len(unread_groups):
            key = RedisKeys.unread_groups(user_id)

            # FakeRedis doesn't support multiple values for SADD
            if self.testing:
                for group_id in unread_groups:
                    p.sadd(key, group_id)
            else:
                p.sadd(key, *unread_groups)

            p.expire(key, ONE_DAY * 2)
        p.execute()

    def decrease_total_unread_message_count(self, user_id: int, amount: int):
        if amount == 0:
            return

        key = RedisKeys.total_unread_count(user_id)
        current_amount = self.redis.get(key)

        # not yet cached, ignore decreasing
        if current_amount is None:
            return

        current_amount = int(float(current_amount))

        # don't set negative unread amount
        if amount > current_amount:
            logger.warning(
                f"after decreasing unread count it became negative for user {user_id}: {current_amount - amount}"
            )
            amount = current_amount

        r = self.redis.pipeline()
        r.decr(key, amount)
        r.expire(key, ONE_DAY * 2)
        r.execute()

    def increase_total_unread_message_count(self, user_ids: List[int], amount: int, pipeline=None):
        for user_id in user_ids:
            key = RedisKeys.total_unread_count(user_id)
            current_cached_unread = self.redis.get(key)

            # if not cached before, don't increase, make a total count next time it's
            # requested, and then it will be cached correctly
            if current_cached_unread is None:
                continue

            self.redis.incrby(key, amount)
            self.redis.expire(key, ONE_DAY * 2)

    def add_unread_group(self, user_ids: List[int], group_id: str, pipeline=None) -> None:
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user_id in user_ids:
            key = RedisKeys.unread_groups(user_id)
            r.sadd(key, group_id)
            r.expire(key, ONE_DAY * 2)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            r.execute()

    def remove_unread_group(self, user_id: int, group_id: str, pipeline=None) -> None:
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        key = RedisKeys.unread_groups(user_id)
        r.srem(key, group_id)
        r.expire(key, ONE_DAY * 2)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            try:
                r.execute()
            except redis.exceptions.ResponseError:
                # if the group is not cached, redis will throw an error
                pass

    def add_unread_groups(self, user_id: int, group_ids: List[str]) -> None:
        p = self.redis.pipeline()

        key = RedisKeys.unread_groups(user_id)
        for group_id in group_ids:
            p.sadd(key, group_id)

        p.expire(key, ONE_DAY * 2)
        p.execute()

    """
    def increase_total_unread_group_count(self, user_id: int):
        key_group = RedisKeys.total_unread_groups(user_id)

        p = self.redis.pipeline()
        p.incr(key_group)
        p.expire(key_group, ONE_DAY * 2)
        p.execute()
    """

    def get_group_exists(self, group_id: str) -> Optional[bool]:
        value = self.redis.get(RedisKeys.group_exists(group_id))
        if value is None:
            return None
        return value == '1'

    def set_group_exists(self, group_id: str, exists: bool) -> None:
        self.redis.set(RedisKeys.group_exists(group_id), '1' if exists else '0')

    def get_sent_message_count_in_group_for_user(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.sent_message_count_in_group(group_id)
        count = self.redis.hget(key, str(user_id))

        if count is None:
            return None

        return int(float(count))

    def set_sent_message_count_in_group_for_user(self, group_id: str, user_id: int, count: int) -> None:
        key = RedisKeys.sent_message_count_in_group(group_id)
        self.redis.hset(key, str(user_id), count)

    def get_last_read_in_group_oldest(self, group_id: str) -> Optional[float]:
        key = RedisKeys.oldest_last_read_time(group_id)
        last_read = self.redis.get(key)
        if last_read is None:
            return

        return float(last_read)

    def set_last_read_in_group_oldest(self, group_id: str, last_read: float) -> None:
        key = RedisKeys.oldest_last_read_time(group_id)
        self.redis.set(key, str(last_read))
        self.redis.expire(key, ONE_DAY * 7)

    def remove_last_read_in_group_oldest(self, group_id: str) -> None:
        key = RedisKeys.oldest_last_read_time(group_id)
        self.redis.delete(key)

    def get_last_read_in_group_for_user(self, group_id: str, user_id: int) -> Optional[float]:
        key = RedisKeys.last_read_time(group_id)
        last_read = self.redis.hget(key, str(user_id))

        if last_read is not None:
            return float(last_read)

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
                last_reads[user_id] = float(last_read)

        return last_reads, not_cached

    def set_last_read_in_group_for_users(
        self, group_id: str, users: Dict[int, float]
    ) -> None:
        key = RedisKeys.last_read_time(group_id)
        p = self.redis.pipeline()

        for user_id, last_read in users.items():
            p.hset(key, str(user_id), last_read)

        p.expire(key, 7 * ONE_DAY)
        p.execute()

    def get_last_read_times_in_group(self, group_id: str) -> Optional[Dict[int, float]]:
        key = RedisKeys.last_read_time(group_id)
        last_reads = self.redis.hgetall(key)

        if not len(last_reads):
            return

        return {
            int(float(user_id)): float(last_read)
            for user_id, last_read in last_reads.items()
        }

    def set_last_read_in_groups_for_user(
        self, group_ids: List[str], user_id: int, last_read: float
    ) -> None:
        p = self.redis.pipeline()

        for group_id in group_ids:
            key = RedisKeys.last_read_time(group_id)
            p.hset(key, str(user_id), last_read)
            p.expire(key, 7 * ONE_DAY)

        p.execute()

    def set_last_read_in_group_for_user(
        self, group_id: str, user_id: int, last_read: float, pipeline=None
    ) -> None:
        key = RedisKeys.last_read_time(group_id)

        # use pipeline if provided
        r = pipeline or self.redis

        r.hset(key, str(user_id), last_read)
        r.expire(key, 7 * ONE_DAY)

    def remove_last_read_in_group_for_user(self, group_id: str, user_id: int, pipeline=None) -> None:
        key = RedisKeys.last_read_time(group_id)

        # use pipeline if provided
        r = pipeline or self.redis
        r.hdel(key, str(user_id))

    def remove_join_time_in_group_for_user(self, group_id: str, user_id: int, pipeline=None) -> None:
        key = RedisKeys.user_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis
        r.hdel(key, str(user_id))

    def increase_unread_in_group_for(self, group_id: str, user_ids: List[int], pipeline=None) -> None:
        key = RedisKeys.unread_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user_id in user_ids:
            r.hincrby(key, str(user_id), 1)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            r.execute()

    def reset_unread_in_groups(self, user_id: int, group_ids: List[str]):
        p = self.redis.pipeline()

        for group_id in group_ids:
            key = RedisKeys.unread_in_group(group_id)
            p.hset(key, str(user_id), 0)

        p.execute()

    def clear_unread_in_group_for_user(self, group_id: str, user_id) -> None:
        key = RedisKeys.unread_in_group(group_id)
        self.redis.hdel(key, str(user_id))

    def get_unread_in_group(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.unread_in_group(group_id)

        n_unread = self.redis.hget(key, str(user_id))
        if n_unread is None:
            return None

        try:
            return int(n_unread)
        except (TypeError, ValueError):
            return None

    def set_unread_in_group(self, group_id: str, user_id: int, unread: int, pipeline=None) -> None:
        key = RedisKeys.unread_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis
        r.hset(key, str(user_id), unread)

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

        messages, until = messages_until.split("|")
        return int(messages), float(until)

    def set_last_message_time_in_group(self, group_id: str, last_message_time: float, pipeline=None):
        key = RedisKeys.last_message_time(group_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        r.set(key, last_message_time)
        r.expire(key, ONE_WEEK)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            r.execute()

    def get_last_message_time_in_group(self, group_id: str):
        key = RedisKeys.last_message_time(group_id)
        last_message_time = self.redis.get(key)

        if last_message_time is None:
            return None

        return float(last_message_time)

    def reset_count_group_types_for_user(self, user_id: int) -> None:
        key = RedisKeys.count_group_types_including_hidden(user_id)
        self.redis.delete(key)

        key = RedisKeys.count_group_types_not_including_hidden(user_id)
        self.redis.delete(key)

    def get_delete_before(self, group_id: str, user_id: int) -> Optional[dt]:
        key = RedisKeys.delete_before(group_id, user_id)
        delete_before = self.redis.get(key)

        if delete_before is not None:
            delete_before = float(delete_before)
            return to_dt(delete_before, allow_none=True)

    def set_delete_before(self, group_id: str, user_id: int, delete_before: float, pipeline=None) -> None:
        key = RedisKeys.delete_before(group_id, user_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        r.set(key, delete_before)
        r.expire(key, 14 * ONE_DAY)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            r.execute()

    def increase_attachment_count_in_group_for_users(self, group_id: str, user_ids: List[int]):
        p = self.redis.pipeline()

        # loop-invariant-global-usage
        two_weeks = ONE_DAY * 14

        for user_id in user_ids:
            key = RedisKeys.attachment_count_group_user(group_id, user_id)
            p.incr(key)
            p.expire(key, two_weeks)

        p.execute()

    def remove_attachment_count_in_group_for_users(self, group_id: str, user_ids: List[int], pipeline=None):
        keys = [
            RedisKeys.attachment_count_group_user(group_id, user_id)
            for user_id in user_ids
        ]

        # use pipeline if provided
        r = pipeline or self.redis
        r.delete(*keys)

    def get_attachment_count_in_group_for_user(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.attachment_count_group_user(group_id, user_id)
        the_count = self.redis.get(key)

        if the_count is None:
            return None

        return int(float(the_count))

    def set_attachment_count_in_group_for_user(self, group_id: str, user_id: int, the_count: int) -> None:
        key = RedisKeys.attachment_count_group_user(group_id, user_id)
        self.redis.set(key, str(the_count))
        self.redis.expire(key, ONE_DAY * 14)

    def set_last_sent_for_user(self, user_id: int, group_id: str, last_time: float, pipeline=None) -> None:
        key = RedisKeys.last_sent_time_user(user_id)

        # use pipeline if provided
        r = pipeline or self.redis

        r.set(key, f"{group_id}:{last_time}")

    def get_last_sent_for_user(self, user_id: int) -> (str, float):
        key = RedisKeys.last_sent_time_user(user_id)
        values = self.redis.get(key)
        if values is None:
            return None, None

        group_id, last_time = values.split(":", maxsplit=1)
        return group_id, float(last_time)

    def set_count_group_types_for_user(self, user_id: int, counts: List[Tuple[int, int]], hidden: bool) -> None:
        if hidden:
            key = RedisKeys.count_group_types_including_hidden(user_id)
        else:
            key = RedisKeys.count_group_types_not_including_hidden(user_id)

        types = ",".join([":".join(map(str, values)) for values in counts])

        self.redis.set(key, types)
        self.redis.expire(key, ONE_DAY * 2)

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

        # TODO: log and check this, is blank in redis quite often
        if len(count) == 0 or "," not in count:
            logger.warning(f"group types was none in cache for key {key}")
            return None

        types = count.split(",")
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

    def remove_user_id_and_join_time_in_groups_for_user(self, group_ids: List[str], user_id: int, pipeline=None):
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()
        user_id = str(user_id)

        for group_id in group_ids:
            key = RedisKeys.user_in_group(group_id)
            r.hdel(key, user_id)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            r.execute()

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
        self, group_id: str, hide: bool, user_ids: List[int] = None, pipeline=None
    ) -> None:
        key = RedisKeys.hide_group(group_id)

        if user_ids is None:
            users = self.redis.hgetall(key)
        else:
            users = user_ids

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user in users:
            r.hset(key, user, "t" if hide else "f")

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            r.execute()

    @property
    def redis(self):
        if self.redis_pool is None:
            return self.redis_instance
        return redis.Redis(connection_pool=self.redis_pool, decode_responses=True)

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
