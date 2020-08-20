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


ENV_KEY_ENVIRONMENT = "DINO_ENVIRONMENT"
gn_environment = os.getenv(ENV_KEY_ENVIRONMENT)

env = create_env(gn_environment)
init_logging(env)
