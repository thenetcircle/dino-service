import os
import logging
from flask import Flask
from flask_socketio import SocketIO
from werkzeug.contrib.fixers import ProxyFix
from dinofw.config import ConfigKeys

logging.basicConfig(level='DEBUG', format=ConfigKeys.DEFAULT_LOG_FORMAT)
logger = logging.getLogger(__name__)
socket_logger = logging.getLogger("socketio")
socket_logger.setLevel(logging.DEBUG)
logging.getLogger("amqp").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)
logging.getLogger("engineio.server").setLevel(logging.WARNING)

from dinofw import environ


def create_app():
    _app = Flask(
        __name__,
        static_folder='templates/static'
    )

    # used for encrypting cookies for handling sessions
    _app.config["SECRET_KEY"] = "abc492ee-9739-11e6-a174-07f6b92d4a4b"

    queue_host = environ.env.config.get(
        ConfigKeys.HOST, domain=ConfigKeys.COORDINATOR, default=""
    )

    message_db = environ.env.config.get(
        ConfigKeys.DB, domain=ConfigKeys.COORDINATOR, default=0
    )
    message_env = environ.env.config.get(ConfigKeys.ENVIRONMENT, default="test")
    message_channel = "dino_{}_{}".format(message_env, message_db)
    message_queue = "redis://{}".format(queue_host)

    logger.info(f"message_queue: {message_queue}")
    cors = environ.env.config.get(ConfigKeys.CORS_ORIGINS, default="*").split(",")
    if cors == ["*"]:
        cors = cors[0]

    _socketio = SocketIO(
        _app,
        logger=socket_logger,
        engineio_logger=os.environ.get("DINO_DEBUG", "0") == "1",
        async_mode="eventlet",
        message_queue=message_queue,
        channel=message_channel,
        cors_allowed_origins=cors,
    )

    # preferably "emit" should be set during env creation, but the socketio object is not created until after env is
    environ.env.out_of_scope_emit = _socketio.emit

    _app.wsgi_app = ProxyFix(_app.wsgi_app)
    return _app, _socketio


app, socketio = create_app()

import dinofw.endpoint.sockets
