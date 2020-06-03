import os
import logging
from gnenv import create_env
from gnenv.environ import GNEnvironment

from dinofw.config import ConfigKeys

logger = logging.getLogger(__name__)


def init_logging(gn_env: GNEnvironment) -> None:
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    logging_type = gn_env.config.get(ConfigKeys.TYPE, domain=ConfigKeys.LOGGING, default='logger')
    if logging_type is None or len(logging_type.strip()) == 0 or logging_type in ['logger', 'default', 'mock']:
        return
    if logging_type != 'sentry':
        raise RuntimeError('unknown logging type %s' % logging_type)

    dsn = gn_env.config.get(ConfigKeys.DSN, domain=ConfigKeys.LOGGING, default='')
    if dsn is None or len(dsn.strip()) == 0:
        logger.warning('sentry logging selected but no DSN supplied, not configuring senty')
        return

    import raven
    import socket
    from git.cmd import Git

    home_dir = os.environ.get('DINO_HOME', default=None)
    if home_dir is None:
        home_dir = '.'
    tag_name = Git(home_dir).describe()

    gn_env.sentry = raven.Client(
        dsn=dsn,
        environment=os.getenv(ENV_KEY_ENVIRONMENT),
        name=socket.gethostname(),
        release=tag_name
    )

    def capture_exception(e_info) -> None:
        try:
            gn_env.sentry.captureException(e_info)
        except Exception as e2:
            logger.exception(e_info)
            logger.error('could not capture exception with sentry: %s' % str(e2))

    gn_env.capture_exception = capture_exception


def init_database(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    from dinofw.db.handler import DatabaseRdbms
    gn_env.db = DatabaseRdbms(gn_env)
    gn_env.db.init_config()


def init_auth_service(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    auth_engine = gn_env.config.get(ConfigKeys.AUTH_SERVICE, None)

    if auth_engine is None:
        raise RuntimeError('no auth service specified')

    auth_type = auth_engine.get(ConfigKeys.TYPE, None)
    if auth_type is None:
        raise RuntimeError('no auth type specified, use one of [redis, nutcracker, allowall, denyall]')

    if auth_type == 'redis' or auth_type == 'nutcracker':
        from dinofw.auth.redis import AuthRedis

        auth_host, auth_port = auth_engine.get(ConfigKeys.HOST), None
        if ':' in auth_host:
            auth_host, auth_port = auth_host.split(':', 1)

        auth_db = auth_engine.get(ConfigKeys.DB, 0)
        gn_env.auth = AuthRedis(host=auth_host, port=auth_port, db=auth_db, env=gn_env)

    elif auth_type == 'allowall':
        from dinofw.auth.simple import AllowAllAuth
        gn_env.auth = AllowAllAuth()

    elif auth_type == 'denyall':
        from dinofw.auth.simple import DenyAllAuth
        gn_env.auth = DenyAllAuth()

    else:
        raise RuntimeError(
            'unknown auth type "{}", use one of [redis, nutcracker, allowall, denyall]'.format(auth_type))


def init_cache_service(gn_env: GNEnvironment):
    if len(gn_env.config) == 0 or gn_env.config.get(ConfigKeys.TESTING, False):
        # assume we're testing
        return

    cache_engine = gn_env.config.get(ConfigKeys.CACHE_SERVICE, None)

    if cache_engine is None:
        raise RuntimeError('no cache service specified')

    cache_type = cache_engine.get(ConfigKeys.TYPE, None)
    if cache_type is None:
        raise RuntimeError('no cache type specified, use one of [redis, nutcracker, memory, missall]')

    if cache_type == 'redis' or cache_type == 'nutcracker':
        from dinofw.cache.redis import CacheRedis

        cache_host, cache_port = cache_engine.get(ConfigKeys.HOST), None
        if ':' in cache_host:
            cache_host, cache_port = cache_host.split(':', 1)

        cache_db = cache_engine.get(ConfigKeys.DB, 0)
        gn_env.cache = CacheRedis(gn_env, host=cache_host, port=cache_port, db=cache_db)

    elif cache_type == 'memory':
        from dinofw.cache.redis import CacheRedis
        gn_env.cache = CacheRedis(gn_env, host='mock')

    elif cache_type == 'missall':
        from dinofw.cache.miss import CacheAllMiss
        gn_env.cache = CacheAllMiss()

    else:
        raise RuntimeError('unknown cache type %s, use one of [redis, nutcracker, memory, missall]' % cache_type)


def initialize_env(dino_env):
    init_logging(dino_env)
    init_database(dino_env)
    init_auth_service(dino_env)
    init_cache_service(dino_env)

    # init_request_validators(dino_env)
    # init_pub_sub(dino_env)
    # init_stats_service(dino_env)
    # init_observer(dino_env)
    # init_response_formatter(dino_env)
    # init_enrichment_service(dino_env)
    # init_storage_engine(dino_env)


ENV_KEY_ENVIRONMENT = "DINO_ENVIRONMENT"
gn_environment = os.getenv(ENV_KEY_ENVIRONMENT)

env = create_env(gn_environment)
initialize_env(env)
