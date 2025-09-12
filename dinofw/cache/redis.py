import socket
import sys
from contextlib import asynccontextmanager
from datetime import datetime as dt
from datetime import timedelta
from typing import Dict, Set
from typing import List
from typing import Optional
from typing import Tuple
import random

import redis
from loguru import logger

from dinofw.cache import ICache
from dinofw.utils import to_dt, split_into_chunks, to_ts
from dinofw.utils.config import ConfigKeys
from dinofw.utils.config import RedisKeys

ONE_MINUTE = 60
FIVE_MINUTES = ONE_MINUTE * 5
ONE_HOUR = ONE_MINUTE * 60
ONE_DAY = 24 * ONE_HOUR
ONE_WEEK = 7 * ONE_DAY


def _to_str(b):
    if isinstance(b, (bytes, bytearray)):
        return b.decode("utf-8")
    return b


def _to_int(b):
    s = _to_str(b)
    return int(s)


def _to_float(b):
    s = _to_str(b)
    return float(s)


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
            from fakeredis import FakeAsyncRedis

            self.redis_pool = None
            self.redis_instance = FakeAsyncRedis(host=host, port=port, db=db, decode_responses=True)

            # fakeredis doesn't use execute on pipelines...
            self.redis_instance.execute = lambda: None

            self.testing = True
        else:
            self.redis_pool = redis.asyncio.ConnectionPool(host=host, port=port, db=db, decode_responses=True)
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
        self.max_client_ids = int(float(
            env.config.get(ConfigKeys.MAX_CLIENT_IDS, domain=ConfigKeys.CACHE_SERVICE, default=10)
        ))

    @asynccontextmanager
    async def pipeline(self):
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
                await p.execute()

    async def get_next_client_id(self, domain: str, user_id: int) -> str:
        key = RedisKeys.client_id(domain, user_id)
        current_idx = await self.redis.llen(key)

        # start from 0 if no pool found
        if current_idx == 0:
            current_idx = -1

        # if we've reached the max 50 ids, reset the pool and restart from 0
        elif current_idx >= self.max_client_ids - 1:
            current_idx = -1
            await self.redis.delete(key)

        next_client_id = f"{user_id}_{domain}_{current_idx+1}"
        await self.redis.lpush(key, next_client_id)

        # try to reuse lower ids by expiring the pool
        await self.redis.expire(key, 6 * ONE_HOUR)

        return next_client_id

    async def get_total_unread_count(self, user_id: int) -> (Optional[int], Optional[int]):
        p = self.redis.pipeline()

        await p.get(RedisKeys.total_unread_count(user_id))
        await p.scard(RedisKeys.unread_groups(user_id))

        try:
            unread_count, unread_groups = await p.execute()
        except redis.exceptions.ResponseError:
            # if no value is cached, SCARD will throw an error
            return None, None

        if unread_count is not None:
            return int(float(unread_count)), int(float(unread_groups))

        return None, None

    async def set_total_unread_count(self, user_id: int, unread_count: int, unread_groups: List[str]) -> None:
        p = self.redis.pipeline()

        key_count = RedisKeys.total_unread_count(user_id)
        await p.set(key_count, unread_count)
        await p.expire(key_count, ONE_HOUR * 12)

        if len(unread_groups):
            key = RedisKeys.unread_groups(user_id)

            # FakeRedis doesn't support multiple values for SADD
            if self.testing:
                for group_id in unread_groups:
                    await p.sadd(key, group_id)
            else:
                await p.sadd(key, *unread_groups)

            await p.expire(key, ONE_HOUR * 12)
        await p.execute()

    async def decrease_total_unread_message_count(self, user_id: int, amount: int):
        if amount == 0:
            return

        key = RedisKeys.total_unread_count(user_id)
        current_amount = await self.redis.get(key)

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
        await r.decr(key, amount)
        await r.expire(key, ONE_HOUR)
        await r.execute()

    async def reset_total_unread_message_count(self, user_id: int, pipeline=None):
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        await r.delete(RedisKeys.total_unread_count(user_id))
        await r.delete(RedisKeys.unread_groups(user_id))

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    async def increase_total_unread_message_count(self, user_ids: List[int], amount: int, pipeline=None):
        for user_id in user_ids:
            key = RedisKeys.total_unread_count(user_id)
            current_cached_unread = await self.redis.get(key)

            # if not cached before, don't increase, make a total count next time it's
            # requested, and then it will be cached correctly
            if current_cached_unread is None:
                continue

            await self.redis.incrby(key, amount)
            await self.redis.expire(key, ONE_HOUR)

    async def add_unread_group(self, user_ids: List[int], group_id: str, pipeline=None) -> None:
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user_id in user_ids:
            key = RedisKeys.unread_groups(user_id)
            await r.sadd(key, group_id)
            await r.expire(key, ONE_HOUR)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    async def remove_unread_group(self, user_id: int, group_id: str, pipeline=None) -> None:
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        key = RedisKeys.unread_groups(user_id)
        await r.srem(key, group_id)
        await r.expire(key, ONE_HOUR)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            try:
                await r.execute()
            except redis.exceptions.ResponseError:
                # if the group is not cached, redis will throw an error
                pass

    async def add_unread_groups(self, user_id: int, group_ids: List[str]) -> None:
        p = self.redis.pipeline()

        key = RedisKeys.unread_groups(user_id)
        for group_id in group_ids:
            await p.sadd(key, group_id)

        await p.expire(key, ONE_HOUR)
        await p.execute()

    """
    def increase_total_unread_group_count(self, user_id: int):
        key_group = RedisKeys.total_unread_groups(user_id)

        p = self.redis.pipeline()
        p.incr(key_group)
        p.expire(key_group, ONE_HOUR)
        p.execute()
    """

    async def get_group_exists(self, group_id: str) -> Optional[bool]:
        value = await self.redis.get(RedisKeys.group_exists(group_id))
        if value is None:
            return None
        return value == '1'

    async def set_group_exists(self, group_id: str, exists: bool) -> None:
        await self.redis.set(RedisKeys.group_exists(group_id), '1' if exists else '0')

    async def get_sent_message_count_in_group_for_user(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.sent_message_count_in_group(group_id)
        count = await self.redis.hget(key, str(user_id))

        if count is None:
            return None

        return int(float(count))

    async def get_group_status(self, group_id: str) -> Optional[int]:
        key = RedisKeys.group_status(group_id)
        status = await self.redis.get(key)

        if status is None:
            return None

        return int(float(status))

    async def count_online(self):
        key = RedisKeys.online_users()

        online_count = self.cache.get(key)
        if online_count is not None:
            return online_count

        online_count = await self.redis.scard(RedisKeys.online_users())
        if online_count is None:
            online_count = 0

        self.cache.set(key, online_count, ttl=30)

        return online_count

    async def is_online(self, user_id: int) -> bool:
        return bool(await self.redis.sismember(RedisKeys.online_users(), str(user_id)))

    async def get_online_users_ttl_expired(self) -> bool:
        key = RedisKeys.online_users() + ":ttl"
        return (await self.redis.ttl(key)) < 0

    async def set_online_users_ttl_expired(self, ttl: int = ONE_MINUTE * 4) -> None:
        key = RedisKeys.online_users() + ":ttl"
        await self.redis.set(key, "1")
        await self.redis.expire(key, ttl)

    async def get_online_users(self) -> Set[int]:
        return {int(user_id) for user_id in await self.redis.smembers(RedisKeys.online_users())}

    async def set_online_users(self, offline: List[int] = None, online: List[int] = None) -> None:
        key = RedisKeys.online_users()
        logger.debug(f"offline: {offline}, online: {online}")

        if offline and len(offline):
            for rem_chunk in split_into_chunks(offline, 100):
                await self.redis.srem(key, *rem_chunk)

        if online and len(online):
            for add_chunk in split_into_chunks(online, 100):
                await self.redis.sadd(key, *add_chunk)

        await self.set_online_users_ttl_expired()

    async def set_online_users_only(self, online: List[int] = None) -> None:
        key = RedisKeys.online_users()

        in_cache = {int(user_id) for user_id in await self.redis.smembers(RedisKeys.online_users())}
        to_remove = in_cache - set(online)

        if len(to_remove):
            for rem_chunk in split_into_chunks(list(to_remove), 100):
                await self.redis.srem(key, *rem_chunk)

        if online and len(online):
            for add_chunk in split_into_chunks(online, 100):
                await self.redis.sadd(key, *add_chunk)

    async def set_online_user(self, user_id: int) -> None:
        await self.redis.sadd(RedisKeys.online_users(), user_id)

    async def set_offline_user(self, user_id: int) -> None:
        await self.redis.srem(RedisKeys.online_users(), user_id)

    async def set_group_status(self, group_id: str, status: int) -> None:
        key = RedisKeys.group_status(group_id)
        await self.redis.set(key, status)
        await self.redis.expire(key, ONE_HOUR)

    async def get_group_archived(self, group_id: str) -> Optional[bool]:
        key = RedisKeys.group_archived(group_id)
        archived = await self.redis.get(key)

        if archived is None:
            return None

        return archived == '1'

    async def set_group_archived(self, group_id: str, archived: bool) -> None:
        key = RedisKeys.group_archived(group_id)
        await self.redis.set(key, '1' if archived else '0')
        await self.redis.expire(key, ONE_HOUR)

    async def set_sent_message_count_in_group_for_user(self, group_id: str, user_id: int, count: int) -> None:
        key = RedisKeys.sent_message_count_in_group(group_id)
        await self.redis.hset(key, str(user_id), str(count))

    async def get_last_read_in_group_oldest(self, group_id: str) -> Optional[float]:
        key = RedisKeys.oldest_last_read_time(group_id)
        last_read = await self.redis.get(key)
        if last_read is None:
            return

        return float(last_read)

    async def set_last_read_in_group_oldest(self, group_id: str, last_read: float) -> None:
        key = RedisKeys.oldest_last_read_time(group_id)
        await self.redis.set(key, str(last_read))
        await self.redis.expire(key, ONE_DAY * 7)

    async def remove_last_read_in_group_oldest(self, group_id: str, pipeline=None) -> None:
        key = RedisKeys.oldest_last_read_time(group_id)
        r = pipeline or self.redis
        await r.delete(key)

    async def get_last_read_in_group_for_user(self, group_id: str, user_id: int) -> Optional[float]:
        key = RedisKeys.last_read_time(group_id)
        last_read = await self.redis.hget(key, str(user_id))

        if last_read is not None:
            return float(last_read)

    async def get_last_read_in_group_for_users(
        self, group_id: str, user_ids: List[int]
    ) -> Tuple[dict, list]:
        p = self.redis.pipeline()

        for user_id in user_ids:
            key = RedisKeys.last_read_time(group_id)
            await p.hget(key, str(user_id))

        not_cached = list()
        last_reads = dict()

        for user_id, last_read in zip(user_ids, await p.execute()):
            if last_read is None:
                not_cached.append(user_id)
            else:
                last_reads[user_id] = float(last_read)

        return last_reads, not_cached

    async def set_last_read_in_group_for_users(
        self, group_id: str, users: Dict[int, float]
    ) -> None:
        key = RedisKeys.last_read_time(group_id)
        await self.redis.delete(key)
        if users:
            mapping = {
                str(user_id): str(last_read)
                for user_id, last_read in users.items()
            }
            await self.redis.hset(key, mapping=mapping)
        await self.redis.expire(key, 7 * ONE_DAY)


    async def get_last_read_times_in_group(self, group_id: str) -> Optional[Dict[int, float]]:
        key = RedisKeys.last_read_time(group_id)
        last_reads = await self.redis.hgetall(key)

        if not len(last_reads):
            return None

        return {
            _to_int(user_id): _to_float(last_read)
            for user_id, last_read in last_reads.items()
        }

    async def set_last_read_in_groups_for_user(
        self, group_ids: List[str], user_id: int, last_read: float
    ) -> None:
        p = self.redis.pipeline()

        for group_id in group_ids:
            key = RedisKeys.last_read_time(group_id)
            await p.hset(key, str(user_id), str(last_read))
            await p.expire(key, 7 * ONE_DAY)

        await p.execute()

    async def set_last_read_in_group_for_user(
        self, group_id: str, user_id: int, last_read: float, pipeline=None
    ) -> None:
        key = RedisKeys.last_read_time(group_id)

        # use pipeline if provided
        r = pipeline or self.redis

        await r.hset(key, str(user_id), str(last_read))
        await r.expire(key, 7 * ONE_DAY)

    async def remove_last_read_in_group_for_user(self, group_id: str, user_id: int, pipeline=None) -> None:
        key = RedisKeys.last_read_time(group_id)

        # use pipeline if provided
        r = pipeline or self.redis
        await r.hdel(key, str(user_id))

    async def remove_join_time_in_group_for_user(self, group_id: str, user_id: int, pipeline=None) -> None:
        key = RedisKeys.user_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis
        await r.hdel(key, str(user_id))

    async def increase_unread_in_group_for(self, group_id: str, user_ids: List[int], pipeline=None) -> None:
        key = RedisKeys.unread_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user_id in user_ids:
            await r.hincrby(key, str(user_id), 1)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    async def reset_unread_in_groups(self, user_id: int, group_ids: List[str]):
        p = self.redis.pipeline()

        for group_id in group_ids:
            key = RedisKeys.unread_in_group(group_id)
            await p.hset(key, str(user_id), "0")

        await p.execute()

    async def clear_unread_in_group_for_user(self, group_id: str, user_id, pipeline=None) -> None:
        key = RedisKeys.unread_in_group(group_id)
        r = pipeline or self.redis
        await r.hdel(key, str(user_id))

    async def get_unread_in_group(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.unread_in_group(group_id)

        n_unread = await self.redis.hget(key, str(user_id))
        if n_unread is None:
            return None

        try:
            return int(n_unread)
        except (TypeError, ValueError):
            return None

    async def set_unread_in_group(self, group_id: str, user_id: int, unread: int, pipeline=None) -> None:
        key = RedisKeys.unread_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis
        await r.hset(key, str(user_id), str(unread))

    async def get_user_count_in_group(self, group_id: str) -> Optional[int]:
        key = RedisKeys.user_in_group(group_id)
        n_users = await self.redis.hlen(key)

        if n_users is None:
            return None

        return n_users

    async def get_messages_in_group(self, group_id: str) -> (Optional[int], Optional[float]):
        key = RedisKeys.messages_in_group(group_id)
        messages_until = await self.redis.get(key)

        if messages_until is None:
            return None, None

        messages, until = messages_until.split("|")
        return int(messages), float(until)

    async def set_last_message_time_in_group(self, group_id: str, last_message_time: float, pipeline=None):
        key = RedisKeys.last_message_time(group_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        await r.set(key, last_message_time)
        await r.expire(key, ONE_WEEK)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    async def get_last_message_time_in_group(self, group_id: str):
        key = RedisKeys.last_message_time(group_id)
        last_message_time = await self.redis.get(key)

        if last_message_time is None:
            return None

        return float(last_message_time)

    async def get_group_type(self, group_id: str) -> Optional[int]:
        key = RedisKeys.group_type(group_id)
        group_type = await self.redis.get(key)

        if group_type is None:
            return None

        return int(group_type)

    async def set_group_type(self, group_id: str, group_type: int) -> None:
        key = RedisKeys.group_type(group_id)
        await self.redis.set(key, group_type)
        await self.redis.expire(key, ONE_DAY)

    async def increase_count_group_types_for_user(self, user_id: int, group_type: int) -> None:
        for is_hidden in {True, False}:
            current_group_types = await self.get_count_group_types_for_user(user_id, hidden=is_hidden)
            if current_group_types is None:
                continue

            new_group_types = list()
            for current_group_type, the_count in current_group_types:
                if current_group_type == group_type:
                    the_count += 1

                new_group_types.append((current_group_type, the_count))

            await self.set_count_group_types_for_user(user_id, new_group_types, is_hidden)

    async def reset_count_group_types_for_user(self, user_id: int) -> None:
        key = RedisKeys.count_group_types_including_hidden(user_id)
        await self.redis.delete(key)

        key = RedisKeys.count_group_types_not_including_hidden(user_id)
        await self.redis.delete(key)

    async def get_delete_before(self, group_id: str, user_id: int) -> Optional[dt]:
        key = RedisKeys.delete_before(group_id, user_id)
        delete_before = await self.redis.get(key)

        if delete_before is not None:
            delete_before = float(delete_before)
            return to_dt(delete_before, allow_none=True)

    async def set_delete_before(self, group_id: str, user_id: int, delete_before: float, pipeline=None) -> None:
        key = RedisKeys.delete_before(group_id, user_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        await r.set(key, delete_before)
        await r.expire(key, 14 * ONE_DAY)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    async def increase_attachment_count_in_group_for_users(self, group_id: str, user_ids: List[int]):
        p = self.redis.pipeline()

        # loop-invariant-global-usage
        two_weeks = ONE_DAY * 14

        for user_id in user_ids:
            key = RedisKeys.attachment_count_group_user(group_id, user_id)
            await p.incr(key)
            await p.expire(key, two_weeks)

        await p.execute()

    async def remove_attachment_count_in_group_for_users(self, group_id: str, user_ids: List[int], pipeline=None):
        keys = [
            RedisKeys.attachment_count_group_user(group_id, user_id)
            for user_id in user_ids
        ]

        # use pipeline if provided
        r = pipeline or self.redis
        await r.delete(*keys)

    async def get_attachment_count_in_group_for_user(self, group_id: str, user_id: int) -> Optional[int]:
        key = RedisKeys.attachment_count_group_user(group_id, user_id)
        the_count = await self.redis.get(key)

        if the_count is None:
            return None

        return int(float(the_count))

    async def set_attachment_count_in_group_for_user(self, group_id: str, user_id: int, the_count: int) -> None:
        key = RedisKeys.attachment_count_group_user(group_id, user_id)
        await self.redis.set(key, str(the_count))
        await self.redis.expire(key, ONE_DAY * 14)

    async def set_last_sent_for_user(self, user_id: int, group_id: str, last_time: float, pipeline=None) -> None:
        key = RedisKeys.last_sent_time_user(user_id)

        # use pipeline if provided
        r = pipeline or self.redis

        await r.set(key, f"{group_id}:{last_time}")

    async def get_last_sent_for_user(self, user_id: int) -> (str, float):
        key = RedisKeys.last_sent_time_user(user_id)
        values = await self.redis.get(key)
        if values is None:
            return None, None

        group_id, last_time = values.split(":", maxsplit=1)
        return group_id, float(last_time)

    async def set_count_group_types_for_user(self, user_id: int, counts: List[Tuple[int, int]], hidden: bool) -> None:
        if hidden:
            key = RedisKeys.count_group_types_including_hidden(user_id)
        else:
            key = RedisKeys.count_group_types_not_including_hidden(user_id)

        types = ",".join([":".join(map(str, values)) for values in counts])

        await self.redis.set(key, types)
        await self.redis.expire(key, ONE_DAY * 5)

    async def get_count_group_types_for_user(self, user_id: int, hidden: bool) -> Optional[List[Tuple[int, int]]]:
        if hidden is None:
            return None

        if hidden:
            key = RedisKeys.count_group_types_including_hidden(user_id)
        else:
            key = RedisKeys.count_group_types_not_including_hidden(user_id)

        count = await self.redis.get(key)
        if count is None:
            return None

        if len(count) == 0:
            logger.warning(f"group types was none in cache for key {key}")
            return None

        types = count.split(",")
        types = [
            group_type.split(":", maxsplit=1)
            for group_type in types
        ]

        return [(int(a), int(b)) for a, b in types]

    async def set_messages_in_group(self, group_id: str, n_messages: int, until: float) -> None:
        key = RedisKeys.messages_in_group(group_id)
        messages_until = f"{n_messages}|{until}"

        await self.redis.set(key, messages_until)
        await self.redis.expire(key, ONE_HOUR)  # can't cache forever, since users may delete historical messages

    async def get_user_ids_and_join_time_in_groups(self, group_ids: List[str]):
        join_times = dict()

        p = self.redis.pipeline()
        for group_id in group_ids:
            await p.hgetall(RedisKeys.user_in_group(group_id))

        for group_id, users in zip(group_ids, await p.execute()):
            if not len(users):
                continue

            join_times[group_id] = {
                int(user_id): float(join_time)
                for user_id, join_time in users.items()
            }

        return join_times

    async def set_user_ids_and_join_time_in_groups(
        self, group_users: Dict[str, Dict[int, float]]
    ):
        p = self.redis.pipeline()

        for group_id, users in group_users.items():
            key = RedisKeys.user_in_group(group_id)
            await p.delete(key)

            if len(users):
                for user_id, join_time in users.items():
                    await p.hset(key, str(user_id), str(join_time))
                await p.expire(key, FIVE_MINUTES)

        await p.execute()

    async def get_user_ids_and_join_time_in_group(
        self, group_id: str
    ) -> Optional[Dict[int, float]]:
        users = await self.redis.hgetall(RedisKeys.user_in_group(group_id))

        if not len(users):
            return None

        return {int(user_id): float(join_time) for user_id, join_time in users.items()}

    async def last_read_was_updated(self, group_id: str, user_id: int, last_read: float) -> None:
        async with self.pipeline() as p:
            await self.set_last_read_in_group_for_user(group_id, user_id, to_ts(last_read), pipeline=p)
            await self.clear_unread_in_group_for_user(group_id, user_id, pipeline=p)
            await self.remove_unread_group(user_id, group_id, pipeline=p)
            await self.reset_total_unread_message_count(user_id, pipeline=p)

    async def set_user_ids_and_join_time_in_group(
        self, group_id: str, users: Dict[int, float]
    ):
        key = RedisKeys.user_in_group(group_id)
        p = self.redis.pipeline()

        await p.delete(key)

        if len(users):
            await self.add_user_ids_and_join_time_in_group(group_id, users, pipeline=p, execute=False)
            await p.expire(key, FIVE_MINUTES + random.randint(0, ONE_MINUTE))

        await p.execute()

    async def remove_user_id_and_join_time_in_groups_for_user(self, group_ids: List[str], user_id: int, pipeline=None):
        # use pipeline if provided
        r = pipeline or self.redis.pipeline()
        user_id = str(user_id)

        for group_id in group_ids:
            key = RedisKeys.user_in_group(group_id)
            await r.hdel(key, user_id)

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    async def add_user_ids_and_join_time_in_group(
        self, group_id: str, users: Dict[int, float], pipeline=None, execute: bool = True
    ) -> None:
        key = RedisKeys.user_in_group(group_id)

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user_id, join_time in users.items():
            await r.hset(key, str(user_id), str(join_time))

        # only execute if we weren't provided a pipeline
        if pipeline is None and execute:
            await r.execute()

    async def clear_user_ids_and_join_time_in_group(self, group_id: str) -> None:
        key = RedisKeys.user_in_group(group_id)
        await self.redis.delete(key)

    async def set_hide_group(
        self, group_id: str, hide: bool, user_ids: List[int] = None, pipeline=None
    ) -> None:
        key = RedisKeys.hide_group(group_id)

        if user_ids is None:
            users = await self.redis.hgetall(key)
        else:
            users = user_ids

        # use pipeline if provided
        r = pipeline or self.redis.pipeline()

        for user in users:
            await r.hset(key, user, "t" if hide else "f")

        # only execute if we weren't provided a pipeline
        if pipeline is None:
            await r.execute()

    @property
    def redis(self):
        if self.redis_pool is None:
            return self.redis_instance
        return redis.asyncio.Redis(connection_pool=self.redis_pool, decode_responses=True)

    async def _flushall(self) -> None:
        await self.redis.flushdb()
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
