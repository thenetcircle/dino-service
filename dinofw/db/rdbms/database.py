from uuid import uuid4

from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dinofw.utils.config import ConfigKeys


async def init_db(env, engine=None):
    if engine is None:
        database_uri = env.config.get(ConfigKeys.URI, domain=ConfigKeys.DB)
        pool_size = int(float(env.config.get(ConfigKeys.POOL_SIZE, default=15, domain=ConfigKeys.DB)))

        connection_args = {
            # disable prepared statements, so we can use pgbouncer in transaction mode
            "statement_cache_size": 0,

            # use a unique name for prepared statements to avoid conflicts
            "prepared_statement_name_func": lambda: f"__asyncpg_{uuid4()}__",
        }

        engine = create_async_engine(
            database_uri,
            connect_args=connection_args,
            echo=False,
            pool_size=pool_size
        )

    env.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine, class_=AsyncSession)
    env.Base = declarative_base()

    async with engine.begin() as conn:
        await conn.run_sync(env.Base.metadata.create_all)
