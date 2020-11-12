import logging
import os

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

# keep this import; even though unused, uvicorn needs it, otherwise it will not start
from dinofw.cron import app  # noqa

environ.env.node = "deleter"

app.run_deletions()
