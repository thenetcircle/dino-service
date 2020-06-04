import logging
from typing import Union

from activitystreams import Activity

from dinofw import environ
from dinofw.config import ErrorCodes as ECodes
from dinofw.config import SessionKeys
from dinofw.utils.activity import ActivityBuilder

logger = logging.getLogger(__name__)


def connect() -> (int, None):
    """
    connect to the server

    :return: {'status_code': 200}
    """
    return ECodes.OK, None


def on_login(data: dict, activity: Activity) -> (int, Union[str, None]):
    """
    event sent directly after a connection has successfully been made, to get the user_id for this connection

    :param data: activity streams format, needs actor.id (user id) and actor.summary (user name)
    :param activity: the parsed activity, supplied by @pre_process decorator, NOT by calling endpoint
    :return: if ok: {'status_code': 200}, else: {'status_code': 400, 'data': '<some error message>'}
    """
    user_id = environ.env.session.get(SessionKeys.user_id.value)
    user_name = environ.env.session.get(SessionKeys.user_name.value)

    response = ActivityBuilder.activity_for_login(
        user_id, user_name, encode_attachments=True, include_history=True,
    )

    environ.env.observer.emit("on_login", (data, activity))
    return ECodes.OK, response


def on_disconnect() -> (int, None):
    """
    when a client disconnects or the server no longer gets a ping response from the client

    :return json if ok, {'status_code': 200}
    """

    """
    user_id = str(environ.env.session.get(SessionKeys.user_id.value))
    try:
        sid = request.sid
    except Exception as e:
        logger.error('could not get sid from request: {}'.format(str(e)))
        logger.exception(traceback.format_exc())
        environ.env.capture_exception(sys.exc_info())
        sid = ''

    data = {
        'verb': 'disconnect',
        'actor': {
            'id': user_id,
            'content': sid
        }
    }

    activity = as_parser(data)
    environ.env.observer.emit('on_disconnect', (data, activity))
    """

    return ECodes.OK, None
