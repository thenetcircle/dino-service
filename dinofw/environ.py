import os
import logging
from gnenv import create_env
from gnenv.environ import GNEnvironment

from dinofw.config import ConfigKeys

from flask_socketio import emit as _flask_emit
from flask_socketio import send as _flask_send
from flask_socketio import join_room as _flask_join_room
from flask_socketio import leave_room as _flask_leave_room
from flask import request as _flask_request
from flask import send_from_directory as _flask_send_from_directory
from flask import render_template as _flask_render_template
from flask import session as _flask_session

logger = logging.getLogger(__name__)


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
            "sentry logging selected but no DSN supplied, not configuring senty"
        )
        return

    import raven
    import socket
    from git.cmd import Git

    home_dir = os.environ.get("DINO_HOME", default=None)
    if home_dir is None:
        home_dir = "."
    tag_name = Git(home_dir).describe()

    gn_env.sentry = raven.Client(
        dsn=dsn,
        environment=os.getenv(ENV_KEY_ENVIRONMENT),
        name=socket.gethostname(),
        release=tag_name,
    )

    def capture_exception(e_info) -> None:
        try:
            gn_env.sentry.captureException(e_info)
        except Exception as e2:
            logger.exception(e_info)
            logger.error(f"could not capture exception with sentry: {str(e2)}")

    gn_env.capture_exception = capture_exception


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

    from dinofw.db.cassandra.handler import CassandraHandler

    gn_env.storage = CassandraHandler(gn_env)
    gn_env.storage.setup_tables()


def init_auth_service(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    auth_engine = gn_env.config.get(ConfigKeys.AUTH_SERVICE, None)

    if auth_engine is None:
        raise RuntimeError("no auth service specified")

    auth_type = auth_engine.get(ConfigKeys.TYPE, None)
    if auth_type is None:
        raise RuntimeError(
            "no auth type specified, use one of [redis, nutcracker, allowall, denyall]"
        )

    if auth_type == "redis" or auth_type == "nutcracker":
        from dinofw.auth.redis import AuthRedis

        auth_host, auth_port = auth_engine.get(ConfigKeys.HOST), None
        if ":" in auth_host:
            auth_host, auth_port = auth_host.split(":", 1)

        auth_db = auth_engine.get(ConfigKeys.DB, 0)
        gn_env.auth = AuthRedis(host=auth_host, port=auth_port, db=auth_db, env=gn_env)

    elif auth_type == "allowall":
        from dinofw.auth.simple import AllowAllAuth

        gn_env.auth = AllowAllAuth()

    elif auth_type == "denyall":
        from dinofw.auth.simple import DenyAllAuth

        gn_env.auth = DenyAllAuth()

    else:
        raise RuntimeError(
            f'unknown auth type "{auth_type}", use one of [redis, nutcracker, allowall, denyall]'
        )


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


def init_flask(gn_env: GNEnvironment):
    # needs to be set later after socketio object has been created
    gn_env.out_of_scope_emit = None

    gn_env.emit = _flask_emit
    gn_env.send = _flask_send
    gn_env.join_room = _flask_join_room
    gn_env.leave_room = _flask_leave_room
    gn_env.render_template = _flask_render_template
    gn_env.send_from_directory = _flask_send_from_directory
    gn_env.session = _flask_session
    gn_env.request = _flask_request


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


def init_response_formatter(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    def get_format_keys() -> list:
        _def_keys = ["status_code", "data", "error"]

        res_format = gn_env.config.get(ConfigKeys.RESPONSE_FORMAT, None)
        if res_format is None:
            logger.info("using default response format, no config specified")
            return _def_keys

        if type(res_format) != str:
            logger.warning(
                'configured response format is of type "%s", using default'
                % str(type(res_format))
            )
            return _def_keys

        if len(res_format.strip()) == 0:
            logger.warning("configured response format is blank, using default")
            return _def_keys

        keys = res_format.split(",")
        if len(keys) != 3:
            logger.warning(
                'configured response format not "<code>,<data>,<error>" but "%s", using default'
                % res_format
            )
            return _def_keys

        for i, key in enumerate(keys):
            if len(key.strip()) == 0:
                logger.warning(
                    'response format key if index %s is blank in "%s", using default'
                    % (str(i), keys)
                )
                return _def_keys
        return keys

    code_key, data_key, error_key = get_format_keys()

    from dinofw.utils.formatter import SimpleResponseFormatter

    gn_env.response_formatter = SimpleResponseFormatter(code_key, data_key, error_key)
    logger.info("configured response formatting as %s" % str(gn_env.response_formatter))


def init_request_validators(gn_env: GNEnvironment) -> None:
    from yapsy.PluginManager import PluginManager

    logging.getLogger("yapsy").setLevel(
        gn_env.config.get(ConfigKeys.LOG_LEVEL, logging.INFO)
    )

    plugin_manager = PluginManager()
    plugin_manager.setPluginPlaces(["dino/validation/events"])
    plugin_manager.collectPlugins()

    gn_env.event_validator_map = dict()
    gn_env.event_validators = dict()

    for pluginInfo in plugin_manager.getAllPlugins():
        plugin_manager.activatePluginByName(pluginInfo.name)
        gn_env.event_validators[pluginInfo.name] = pluginInfo.plugin_object

    validation = gn_env.config.get(ConfigKeys.VALIDATION, None)
    if validation is None:
        return

    for key in validation.keys():
        if key not in gn_env.event_validator_map:
            gn_env.event_validator_map[key] = list()
        plugins = validation[key].copy()
        validation[key] = dict()
        for plugin_info in plugins:
            plugin_name = plugin_info.get("name")
            validation[key][plugin_name] = plugin_info
            try:
                gn_env.event_validator_map[key].append(
                    gn_env.event_validators[plugin_name]
                )
            except KeyError:
                raise KeyError('specified plugin "%s" does not exist' % key)

    gn_env.config.set(ConfigKeys.VALIDATION, validation)

    for pluginInfo in plugin_manager.getAllPlugins():
        pluginInfo.plugin_object.setup(gn_env)


def init_observer(gn_env: GNEnvironment) -> None:
    from pymitter import EventEmitter

    gn_env.observer = EventEmitter()


def init_rest(gn_env: GNEnvironment) -> None:
    from dinofw.rest.server.groups import GroupResource
    from dinofw.rest.server.users import UserResource
    from dinofw.rest.server.message import MessageResource

    class RestResources:
        group: GroupResource
        user: UserResource
        message: MessageResource

    gn_env.rest = RestResources()
    gn_env.rest.group = GroupResource(gn_env)
    gn_env.rest.user = UserResource(gn_env)
    gn_env.rest.message = MessageResource(gn_env)


def init_producer(gn_env: GNEnvironment) -> None:
    from dinofw.utils.publisher import Publisher

    gn_env.publisher = Publisher(gn_env, mock=True)  # TODO: don't mock


def initialize_env(dino_env):
    logging.basicConfig(level="DEBUG", format=ConfigKeys.DEFAULT_LOG_FORMAT)

    init_flask(dino_env)
    init_logging(dino_env)
    init_database(dino_env)
    init_cassandra(dino_env)
    init_auth_service(dino_env)
    init_cache_service(dino_env)
    init_stats_service(dino_env)
    init_response_formatter(dino_env)
    init_request_validators(dino_env)
    init_observer(dino_env)
    init_rest(dino_env)
    init_producer(dino_env)


ENV_KEY_ENVIRONMENT = "DINO_ENVIRONMENT"
gn_environment = os.getenv(ENV_KEY_ENVIRONMENT)

env = create_env(gn_environment)
initialize_env(env)
