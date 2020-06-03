from activitystreams import Activity

from dinofw.server import socketio
from dinofw import api
from dinofw import environ
from flask_socketio import disconnect

import logging
import traceback
import sys

from dinofw.utils.decorators import respond_with, pre_process

logger = logging.getLogger(__name__)


@socketio.on("connect", namespace="/")
@respond_with("gn_connect")
def connect() -> (int, None):
    # no pre-processing for connect event
    return api.connect()


@socketio.on('login', namespace='/')
@respond_with('gn_login', should_disconnect=True)
@pre_process('on_login', should_validate_request=False)
def on_login(data: dict, activity: Activity) -> (int, str):
    try:
        status_code, msg = api.on_login(data, activity)
        if status_code != 200:
            disconnect()
        return status_code, msg
    except Exception as e:
        logger.error(f'could not login, will disconnect client: {str(e)}')
        logger.exception(traceback.format_exc())
        environ.env.capture_exception(sys.exc_info())
        return 500, str(e)


@socketio.on('disconnect', namespace='/')
def on_disconnect() -> (int, None):
    # no pre-processing for disconnect event
    return api.on_disconnect()
