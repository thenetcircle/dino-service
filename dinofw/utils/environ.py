import os

from gnenv import create_env
from gnenv.environ import GNEnvironment
from loguru import logger

from dinofw.utils.config import ConfigKeys


def init_logging(gn_env: GNEnvironment) -> None:
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    logging_type = gn_env.config.get(
        ConfigKeys.TYPE, domain=ConfigKeys.LOGGING, default="logger"
    )
    if (
        logging_type is None
        or len(logging_type.strip()) == 0
        or logging_type in ["logger", "default", "mock"]
    ):
        return

    if logging_type != "sentry":
        raise RuntimeError(f"unknown logging type {logging_type}")

    dsn = gn_env.config.get(ConfigKeys.DSN, domain=ConfigKeys.LOGGING, default="")
    if dsn is None or len(dsn.strip()) == 0:
        logger.warning(
            "sentry logging selected but no DSN supplied, not configuring sentry"
        )
        return

    import socket

    home_dir = os.environ.get("DINO_HOME", default=None)
    if home_dir is None:
        home_dir = "."

    tag_name = "unknown"
    version_path = os.path.join(home_dir, "version.txt")

    if os.path.exists(version_path):
        logger.info(f"reading version file '{version_path}'...")
        with open("version.txt", "r") as f:
            tag_name = f.readline().replace("\n", "").strip()
    else:
        logger.warning(f"'{version_path}' file found")

    import sentry_sdk
    from sentry_sdk import capture_exception as sentry_capture_exception
    from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration
    from sentry_sdk.integrations.redis import RedisIntegration

    logger.info(f"initializing sentry sdk with version '{tag_name}'")
    sentry_sdk.init(
        dsn=dsn,
        environment=os.getenv("ENVIRONMENT"),  # TODO: fix DINO_ENVIRONMENT / ENVIRONMENT discrepancy
        server_name=socket.gethostname(),
        release=tag_name,
        integrations=[
            SqlalchemyIntegration(),
            RedisIntegration()
        ],
    )

    def capture_wrapper(e_info) -> None:
        try:
            sentry_capture_exception(e_info)
        except Exception as e2:
            logger.exception(e_info)
            logger.error(f"could not capture exception with sentry: {str(e2)}")

    gn_env.capture_exception = capture_wrapper


def init_database(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    from dinofw.db.rdbms.database import init_db as init_sql_alchemy
    init_sql_alchemy(gn_env)

    from dinofw.db.rdbms.handler import RelationalHandler
    gn_env.db = RelationalHandler(gn_env)


def init_cassandra(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    from dinofw.db.storage.handler import CassandraHandler

    gn_env.storage = CassandraHandler(gn_env)
    gn_env.storage.setup_tables()


def init_cache_service(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    cache_engine = gn_env.config.get(ConfigKeys.CACHE_SERVICE, None)

    if cache_engine is None:
        raise RuntimeError("no cache service specified")

    cache_type = cache_engine.get(ConfigKeys.TYPE, None)
    if cache_type is None:
        raise RuntimeError(
            "no cache type specified, use one of [redis, nutcracker, memory, missall]"
        )

    if cache_type == "redis" or cache_type == "nutcracker":
        from dinofw.cache.redis import CacheRedis

        cache_host, cache_port = cache_engine.get(ConfigKeys.HOST), None
        if ":" in cache_host:
            cache_host, cache_port = cache_host.split(":", 1)

        cache_db = cache_engine.get(ConfigKeys.DB, 0)
        gn_env.cache = CacheRedis(gn_env, host=cache_host, port=cache_port, db=cache_db)

    elif cache_type == "memory":
        from dinofw.cache.redis import CacheRedis

        gn_env.cache = CacheRedis(gn_env, host="mock")

    elif cache_type == "missall":
        from dinofw.cache.miss import CacheAllMiss

        gn_env.cache = CacheAllMiss()

    else:
        raise RuntimeError(
            f"unknown cache type {cache_type}, use one of [redis, nutcracker, memory, missall]"
        )


def init_stats_service(gn_env: GNEnvironment) -> None:
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    stats_engine = gn_env.config.get(ConfigKeys.STATS_SERVICE, None)

    if stats_engine is None:
        raise RuntimeError("no stats service specified")

    stats_type = stats_engine.get(ConfigKeys.TYPE, None)
    if stats_type is None:
        raise RuntimeError(
            "no stats type specified, use one of [statsd] (set host to mock if no stats service wanted)"
        )

    if stats_type == "statsd":
        from dinofw.stats.statsd import StatsdService

        gn_env.stats = StatsdService(gn_env)
        gn_env.stats.set("connections", 0)

    elif stats_type == "mock":
        from dinofw.stats.statsd import MockStatsd

        gn_env.stats = MockStatsd()
        gn_env.stats.set("connections", 0)


def init_rest(gn_env: GNEnvironment) -> None:
    from dinofw.rest.groups import GroupResource
    from dinofw.rest.users import UserResource
    from dinofw.rest.message import MessageResource
    from dinofw.rest.broadcast import BroadcastResource

    class RestResources:
        group: GroupResource
        user: UserResource
        message: MessageResource

    gn_env.rest = RestResources()
    gn_env.rest.group = GroupResource(gn_env)
    gn_env.rest.user = UserResource(gn_env)
    gn_env.rest.message = MessageResource(gn_env)
    gn_env.rest.broadcast = BroadcastResource(gn_env)


def _get_pub_host_port_db(gn_env: GNEnvironment) -> (str, int, int):
    pub_host, pub_port = gn_env.config.get(ConfigKeys.HOST, domain=ConfigKeys.PUBLISHER), None
    pub_db = gn_env.config.get(ConfigKeys.DB, domain=ConfigKeys.PUBLISHER, default=0)

    if ":" in pub_host:
        pub_host, pub_port = pub_host.split(":", 1)

    return pub_host, pub_port, pub_db


def init_producer(gn_env: GNEnvironment) -> None:
    from dinofw.endpoint.mqtt import MqttPublishHandler
    from dinofw.endpoint.kafka import KafkaPublishHandler

    gn_env.client_publisher = MqttPublishHandler(gn_env)
    gn_env.server_publisher = KafkaPublishHandler(gn_env)


def initialize_env(dino_env):
    is_deleter_service = os.getenv("DINO_DELETER") is not None

    init_logging(dino_env)
    init_database(dino_env)
    init_cassandra(dino_env)
    init_cache_service(dino_env)

    if not is_deleter_service:
        init_stats_service(dino_env)
        init_rest(dino_env)
        init_producer(dino_env)


ENV_KEY_ENVIRONMENT = "DINO_ENVIRONMENT"
gn_environment = os.getenv(ENV_KEY_ENVIRONMENT)

env = create_env(gn_environment)

if not env.config.get(ConfigKeys.TESTING, False) and os.getenv(ConfigKeys.TESTING, "0") != "1":
    initialize_env(env)
