import arrow

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
