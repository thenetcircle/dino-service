import arrow
import time

from dinofw.rest.models import CreateGroupQuery
from dinofw.utils import utcnow_dt
from dinofw.utils.config import GroupTypes
from test.base import BaseTest
from test.functional.base_db import BaseDatabaseTest


class TestDatabaseQueries(BaseDatabaseTest):
    def test_get_last_sent_for_user_no_ugs(self):
        session = self.env.session_maker()
        group_id, last_sent = self.env.db.get_last_sent_for_user(
            BaseTest.USER_ID, session
        )
        self.assertIsNone(group_id)
        self.assertIsNone(last_sent)

    def test_get_last_sent_for_user_with_ugs(self):
        session = self.env.session_maker()
        now = arrow.utcnow().datetime

        ugs = self.env.db._create_user_stats(BaseTest.GROUP_ID, BaseTest.USER_ID, now)
        session.add(ugs)
        session.commit()

        group_id, last_sent = self.env.db.get_last_sent_for_user(
            BaseTest.USER_ID, session
        )
        self.assertEqual(BaseTest.GROUP_ID, group_id)
        self.assertIsNotNone(last_sent)

    def test_get_group_ids_and_created_at_for_user(self):
        session = self.env.session_maker()

        user_id = 50
        receivers = [51, 52, 53, 54, 55]

        groups = dict()
        now = utcnow_dt()

        for receiver_id in receivers:
            time.sleep(0.01)
            group = self.env.db.create_group_for_1to1(user_id, receiver_id, now, session)
            groups[group.group_id] = group.created_at

        group_and_created_at = self.env.db.get_group_ids_and_created_at_for_user(user_id, session)
        self.assertEqual(len(group_and_created_at), len(receivers))

        for group_id, created_at in group_and_created_at:
            self.assertEqual(created_at, groups[group_id])

    def test_get_groups_with_undeleted_messagesm(self):
        session = self.env.session_maker()

        users = [50, 51, 52, 53, 54, 55]
        groups = list()

        for i in list(range(5)):
            time.sleep(0.01)
            query = CreateGroupQuery(
                users=users,
                group_name=f"test group {i}",
                group_type=GroupTypes.GROUP,
            )

            now = utcnow_dt()
            group_base = self.env.db.create_group(users[0], query, now, session)
            groups.append(group_base)
