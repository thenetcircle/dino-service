import arrow
import datetime

from dinofw.rest.queries import CreateGroupQuery
from dinofw.utils import utcnow_dt, users_to_group_id, trim_micros
from dinofw.utils.config import GroupTypes
from dinofw.utils.exceptions import UserStatsOrGroupAlreadyCreated, NoSuchGroupException
from test.base import BaseTest
from test.functional.base_functional import BaseServerRestApi


class TestCreateGroup(BaseServerRestApi):
    @BaseServerRestApi.init_db_session
    async def test_create_group_that_exists(self):
        session = self.env.db_session

        await self.send_1v1_message(
            user_id=BaseTest.USER_ID,
            receiver_id=BaseTest.OTHER_USER_ID
        )

        query = CreateGroupQuery(
            group_name="some name",
            group_type=GroupTypes.ONE_TO_ONE,
            users=[BaseTest.USER_ID, BaseTest.OTHER_USER_ID]
        )

        with self.assertRaises(UserStatsOrGroupAlreadyCreated):
            await self.env.db.create_group(
                owner_id=BaseTest.USER_ID,
                query=query,
                utc_now=utcnow_dt(),
                db=session
            )

    @BaseServerRestApi.init_db_session
    async def test_create_with_users_that_exists(self):
        session = self.env.db_session

        utc_now = utcnow_dt()
        group_id = users_to_group_id(BaseTest.USER_ID, BaseTest.OTHER_USER_ID)
        created_at = trim_micros(arrow.get(utc_now).shift(seconds=-1).datetime)
        delete_before = created_at - datetime.timedelta(seconds=1)

        # group_id: str, user_id: int, default_dt: dt, group_type: int, delete_before: dt = None
        session.add(await self.env.db._create_user_stats(
            group_id=group_id,
            user_id=BaseTest.USER_ID,
            default_dt=created_at,
            group_type=GroupTypes.PRIVATE_GROUP,
            delete_before=delete_before
        ))
        session.add(await self.env.db._create_user_stats(
            group_id=group_id,
            user_id=BaseTest.OTHER_USER_ID,
            default_dt=created_at,
            group_type=GroupTypes.PRIVATE_GROUP,
            delete_before=delete_before
        ))
        await session.commit()

        query = CreateGroupQuery(
            group_name="some name",
            group_type=GroupTypes.ONE_TO_ONE,
            users=[BaseTest.USER_ID, BaseTest.OTHER_USER_ID]
        )

        with self.assertRaises(UserStatsOrGroupAlreadyCreated):
            await self.env.db.create_group(
                owner_id=BaseTest.USER_ID,
                query=query,
                utc_now=utcnow_dt(),
                db=session
            )

    @BaseServerRestApi.init_db_session
    async def test_get_group_fails_not_create_user_fails(self):
        session = self.env.db_session

        utc_now = utcnow_dt()
        group_id = users_to_group_id(BaseTest.USER_ID, BaseTest.OTHER_USER_ID)
        created_at = trim_micros(arrow.get(utc_now).shift(seconds=-1).datetime)
        delete_before = created_at - datetime.timedelta(seconds=1)

        session.add(await self.env.db._create_user_stats(
            group_id=group_id,
            user_id=BaseTest.USER_ID,
            default_dt=created_at,
            group_type=GroupTypes.PRIVATE_GROUP,
            delete_before=delete_before
        ))
        session.add(await self.env.db._create_user_stats(
            group_id=group_id,
            user_id=BaseTest.OTHER_USER_ID,
            default_dt=created_at,
            group_type=GroupTypes.PRIVATE_GROUP,
            delete_before=delete_before
        ))
        await session.commit()

        with self.assertRaises(NoSuchGroupException):
            await self.env.rest.message._get_or_create_group_for_1v1(
                BaseTest.USER_ID,
                BaseTest.OTHER_USER_ID,
                session
            )
