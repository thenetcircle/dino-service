import logging

from activitystreams import Activity

from dinofw import environ
from dinofw.config import ErrorCodes as ECodes
from dinofw.config import SessionKeys
from dinofw.validation.base import BaseValidator

logger = logging.getLogger(__name__)


class RequestValidator(BaseValidator):
    def on_msg_status(self, _: Activity) -> (bool, int, str):
        return True, None, None

    def on_login(self, activity: Activity) -> (bool, int, str):
        user_id = activity.actor.id

        if (
            hasattr(activity.actor, "attachments")
            and activity.actor.attachments is not None
        ):
            for attachment in activity.actor.attachments:
                environ.env.session[attachment.object_type] = attachment.content

        if SessionKeys.token.value not in environ.env.session:
            logger.warning(
                "no token in session when logging in for user id %s" % str(user_id)
            )
            return False, ECodes.NO_USER_IN_SESSION, "no token in session"

        token = environ.env.session.get(SessionKeys.token.value)
        is_valid, error_msg, session = self.validate_login(user_id, token)

        if not is_valid:
            logger.warning(
                "login is not valid for user id %s: %s" % (str(user_id), str(error_msg))
            )
            environ.env.stats.incr("on_login.failed")
            return False, ECodes.NOT_ALLOWED, error_msg

        for session_key, session_value in session.items():
            environ.env.session[session_key] = session_value

        return True, None, None

    def on_disconnect(self, activity: Activity) -> (bool, int, str):
        user_id = environ.env.session.get(SessionKeys.user_id.value)
        user_name = environ.env.session.get(SessionKeys.user_name.value)
        if user_id is None or not isinstance(user_id, str) or user_name is None:
            return False, ECodes.NO_USER_IN_SESSION, "no user in session, not connected"
        return True, None, None
