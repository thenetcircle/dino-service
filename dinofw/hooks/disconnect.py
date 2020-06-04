import logging
import sys
import traceback

from dinofw import environ

logger = logging.getLogger(__name__)


class OnDisconnectHooks:
    @staticmethod
    def handle_disconnect(arg: tuple) -> None:
        """
        when a client disconnects this hook will handle the related logic

        :param arg: tuple of (data, parsed_activity)
        :return: nothing
        """
        data, activity = arg
        user_id = activity.actor.id

        if user_id is None or len(user_id.strip()) == 0:
            return

        try:
            environ.env.leave_room(user_id)
        except Exception as e:
            logger.error("could not leave private room: %s" % str(e))
            logger.debug("request for failed leave_private_room(): %s" % str(data))
            logger.exception(traceback.format_exc())
            environ.env.capture_exception(sys.exc_info())


@environ.env.observer.on("on_disconnect")
def _on_disconnect_handle_disconnect(arg: tuple) -> None:
    OnDisconnectHooks.handle_disconnect(arg)
