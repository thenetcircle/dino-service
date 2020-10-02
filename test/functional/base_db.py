import os

from dinofw.utils.config import ConfigKeys
from test.base import BaseTest

os.environ[ConfigKeys.TESTING] = "1"

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dinofw.db.rdbms.database import init_db
from test.mocks import FakeEnv
from dinofw.restful import app
from dinofw.restful import get_db


class BaseDatabaseTest(BaseTest):
    def setUp(self) -> None:
        from gnenv.environ import ConfigDict
        from gnenv.environ import find_config
        from gnenv.environ import load_secrets_file

        config_dict, config_path = find_config("..")
        config_dict = load_secrets_file(
            config_dict, secrets_path="../secrets", env_name="test"
        )
        config = ConfigDict(config_dict)

        db_uri = config.get(ConfigKeys.URI, domain=ConfigKeys.DB)
        engine = create_engine(db_uri, connect_args={"options": "-c timezone=utc"})

        self.env = FakeEnv()

        # need to replace the global environ.env with our FakeEnv, functional model files import it directly
        from dinofw.utils import environ

        environ.env = self.env

        # init with our test db
        init_db(self.env, engine)

        TestingSessionLocal = sessionmaker(
            autocommit=False, autoflush=False, bind=engine
        )

        def override_get_db():
            try:
                db = TestingSessionLocal()
                yield db
            finally:
                db.close()

        # need to use our testing session instead of the real session
        app.dependency_overrides[get_db] = override_get_db

        from dinofw.db.rdbms.handler import RelationalHandler
        from dinofw.db.rdbms import models

        self.env.db = RelationalHandler(self.env)
        self.env.session_maker = TestingSessionLocal

        def clear_test_db():
            db = None

            try:
                db = TestingSessionLocal()
                db.query(models.GroupEntity).delete()
                db.query(models.UserGroupStatsEntity).delete()
                db.commit()
            finally:
                if db is not None:
                    db.close()

        if "test" in db_uri:
            clear_test_db()

        # this is the client we'll be using to call the rest apis in the test cases
        self.client = TestClient(app)
