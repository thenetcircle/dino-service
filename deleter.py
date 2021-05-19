import logging
import os

from dinofw.utils.config import ConfigKeys

logging.basicConfig(
    format=ConfigKeys.DEFAULT_LOG_FORMAT,
    datefmt=ConfigKeys.DEFAULT_DATE_FORMAT,
    level=logging.DEBUG
)

logging.getLogger("cassandra").setLevel(logging.INFO)
logging.getLogger("gmqtt").setLevel(logging.WARNING)
logging.getLogger("kafka").setLevel(logging.INFO)

CQL_ALLOW_MNG = "CQLENG_ALLOW_SCHEMA_MANAGEMENT"
DELETER_KEY = "DINO_DELETER"

# don't allow deleter to change schema
os.environ[CQL_ALLOW_MNG] = "0"

# indicate this is the deleter service (won't initialize unnecessary modules)
os.environ[DELETER_KEY] = "1"

from dinofw.utils import environ
environ.env.node = "deleter"

# keep this import; even though unused, uvicorn needs it, otherwise it will not start
from dinofw.cron import app  # noqa
