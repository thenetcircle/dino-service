from typing import List, Set

import arrow
from fastapi import FastAPI
from loguru import logger

from dinofw.db.rdbms.schemas import GroupBase, UserGroupStatsBase
from dinofw.utils import environ


class Restorer:
    def __init__(self, env):
        self.env = env

        logger.info("initializing Restorer...")

    def run(self):
        logger.info("fetching groups without users...")
        session = environ.env.SessionLocal()

        groups = self.env.db.get_groups_without_users(session)
        if len(groups) == 0:
            logger.info("no groups without users, exiting!")
            return

        groups_to_fix, existing_user_ids = self.get_groups_and_users_to_fix(groups, session)
        logger.info(f"existing_user_ids: {existing_user_ids}")
        logger.info(f"groups_to_fix: {[group.group_id for group in groups_to_fix]}")
        if not len(existing_user_ids):
            logger.info("no existing users left to fix")
            return

        stats_to_create = self.get_stats_to_create(groups_to_fix, existing_user_ids)
        if not len(stats_to_create):
            logger.info("no stats needs to be created")
            return

        logger.info(f"about to create {len(stats_to_create)} stats")
        self.env.db.create_stats_for(stats_to_create, session)

    def get_groups_and_users_to_fix(self, groups: List[GroupBase], session) -> (List[GroupBase], Set[int]):
        unique_user_ids = set()
        groups_to_fix = list()

        for group in groups:
            if "," not in group.name:
                continue

            user_ids = group.name.split(",")
            if len(user_ids) > 2:
                continue

            try:
                user_a, user_b = int(float(user_ids[0])), int(float(user_ids[1]))
            except Exception as e:
                logger.error(f"invalid user ids in group name '{group.name}': {str(e)}")
                continue

            if user_a <= 0 or user_a > 100000000:
                continue
            if user_b <= 0 or user_b > 100000000:
                continue

            unique_user_ids.add(user_a)
            unique_user_ids.add(user_b)
            groups_to_fix.append(group)

        return groups_to_fix, self.env.db.get_existing_user_ids_out_of(unique_user_ids, session)

    def get_stats_to_create(self, groups_to_fix: List[GroupBase], existing_user_ids: Set[int]):
        stats_to_create = list()
        now_dt = arrow.utcnow().datetime

        for group in groups_to_fix:
            user_to_fix = None
            user_ids = map(int, map(float, group.name.split(",")))

            for user in user_ids:
                if user in existing_user_ids:
                    user_to_fix = user
                    break

            logger.info(f"user_ids: {user_ids}")
            logger.info(f"user_to_fix: {user_to_fix}")
            if user_to_fix is None:
                logger.info(f"not fixing group {group.group_id} ({group.name}), both users deleted their profile")
                continue

            stats_to_create.append(UserGroupStatsBase(
                group_id=group.group_id,
                user_id=user_to_fix,
                last_read=now_dt,
                last_sent=group.last_message_time,
                delete_before=group.first_message_time,
                join_time=group.first_message_time,
                highlight_time=None,
                last_updated_time=group.updated_at,
                first_sent=None,
                receiver_highlight_timep=None,
                sent_message_count=-1,
                unread_count=0,
                deleted=False,
                hide=False,
                pin=False,
                bookmark=False,
                rating=None
            ))

        return stats_to_create


restorer = Restorer(environ.env)
app = FastAPI()


@app.post("/v1/run")
def run_deletions():
    restorer.run()
