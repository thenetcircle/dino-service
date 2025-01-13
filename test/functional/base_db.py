import os
from pathlib import Path

from dinofw.utils.config import ConfigKeys
from test.base import BaseTest

os.environ[ConfigKeys.TESTING] = "1"

from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from dinofw.db.rdbms.database import init_db
from test.mocks import FakeEnv
from dinofw.restful import app
from dinofw.utils.api import get_db


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent


class BaseDatabaseTest(BaseTest):
    async def asyncSetUp(self) -> None:
        from gnenv.environ import ConfigDict
        from gnenv.environ import find_config
        from gnenv.environ import load_secrets_file

        config_dict, config_path = find_config(str(get_project_root()))
        config_dict = load_secrets_file(
            config_dict, secrets_path=os.path.join(str(get_project_root()), "secrets"), env_name="test"
        )
        config = ConfigDict(config_dict)

        db_uri = config.get(ConfigKeys.URI, domain=ConfigKeys.DB)
        engine = create_async_engine(db_uri)

        self.env = FakeEnv()
        self.long_ago = 789000000.0  # default value for many timestamps

        # need to replace the global environ.env with our FakeEnv, functional model files import it directly
        from dinofw.utils import environ
        environ.env = self.env

        # init with our test db
        await init_db(self.env, engine)

        TestingSessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
        )

        async def override_get_db():
            try:
                db = TestingSessionLocal()
                yield db
            finally:
                await db.close()

        # need to use our testing session instead of the real session
        app.dependency_overrides[get_db] = override_get_db

        from dinofw.db.rdbms.handler import RelationalHandler
        from dinofw.db.rdbms import models

        self.env.db = RelationalHandler(self.env)
        self.env.session_maker = TestingSessionLocal

        async def clear_test_db():
            session = None

            def _clear_test_db(db):
                db.query(models.GroupEntity).delete()
                db.query(models.UserGroupStatsEntity).delete()
                db.query(models.DeletedStatsEntity).delete()

            try:
                session = TestingSessionLocal()
                await session.run_sync(_clear_test_db)
                await session.commit()
            finally:
                if session is not None:
                    await session.close()


        if "test" in db_uri:
            await clear_test_db()

        # this is the client we'll be using to call the rest apis in the test cases
        self.client = AsyncClient(transport=ASGITransport(app), base_url="http://testserver")

    # no json parameter for AsyncClient.delete
    async def client_delete(self, url: str, json):
        return await self.client.request(
            "DELETE",
            url,
            json=json
        )

    @classmethod
    # decorator for closing db session after each test
    def init_db_session(cls, coro):
        async def decorator(self, *args, **kwargs):
            if not hasattr(self.env, "db_session") or self.env.db_session is None:
                self.env.db_session = self.env.session_maker()
                try:
                    return await coro(self, *args, **kwargs)
                finally:
                    await self.env.db_session.close()
                    self.env.db_session = None
        return decorator
