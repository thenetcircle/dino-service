from dinofw.utils.config import RedisKeys
from test.functional.base_functional import BaseServerRestApi


class TestUsersInGroups(BaseServerRestApi):

    @BaseServerRestApi.init_db_session
    async def test_get_user_ids_in_groups(self):
        def assert_users_in_groups(group_to_users: dict):
            self.assertEqual(4, len(group_to_users.keys()))
            self.assertEqual(2, len(group_to_users[group_ids[0]]))
            self.assertEqual(2, len(group_to_users[group_ids[1]]))
            self.assertEqual(2, len(group_to_users[group_ids[2]]))
            self.assertEqual(2, len(group_to_users[group_ids[3]]))

            self.assertIn(1234, group_to_users[group_ids[0]])
            self.assertIn(4444, group_to_users[group_ids[0]])

            self.assertIn(2345, group_to_users[group_ids[1]])
            self.assertIn(4444, group_to_users[group_ids[1]])

            self.assertIn(3456, group_to_users[group_ids[2]])
            self.assertIn(4444, group_to_users[group_ids[2]])

            self.assertIn(4567, group_to_users[group_ids[3]])
            self.assertIn(4444, group_to_users[group_ids[3]])

        session = self.env.db_session

        group_ids = [
            (await self.send_1v1_message(user_id=1234, receiver_id=4444))["group_id"],
            (await self.send_1v1_message(user_id=2345, receiver_id=4444))["group_id"],
            (await self.send_1v1_message(user_id=3456, receiver_id=4444))["group_id"],
            (await self.send_1v1_message(user_id=4567, receiver_id=4444))["group_id"]
        ]

        # should be cached when sending messages
        self.assertEqual(4, len(self.env.cache.get_user_ids_and_join_time_in_groups(group_ids)))
        _group_to_users = await self.env.db.get_user_ids_in_groups(group_ids, db=session)
        assert_users_in_groups(_group_to_users)

        # delete from cache and get from db
        for group_id in group_ids:
            self.env.cache.redis.delete(RedisKeys.user_in_group(group_id))

        self.assertEqual(0, len(self.env.cache.get_user_ids_and_join_time_in_groups(group_ids)))
        _group_to_users = await self.env.db.get_user_ids_in_groups(group_ids, db=session)
        assert_users_in_groups(_group_to_users)

        # should be cached again now
        self.assertEqual(4, len(self.env.cache.get_user_ids_and_join_time_in_groups(group_ids)))
