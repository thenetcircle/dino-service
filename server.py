import logging
from dinofw.utils import environ

# keep this import; even though unused, uvicorn needs it, otherwise it will not start

logging.getLogger("kafka").setLevel(logging.INFO)

environ.env.node = "app"
