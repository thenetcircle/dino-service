import logging
import traceback
from yapsy.IPlugin import IPlugin
from activitystreams.models.activity import Activity

from dinofw import utils
from dinofw.config import ErrorCodes
from dinofw.config import ConfigKeys
from dinofw.environ import GNEnvironment

logger = logging.getLogger(__name__)


class OnLoginEnforceSingleSession(IPlugin):
    def __init__(self):
        super(OnLoginEnforceSingleSession, self).__init__()
        self.env = None
        self.enabled = False

    def setup(self, env: GNEnvironment):
        self.env = env

    def _process(self, data: dict, activity: Activity):
        user_id = activity.actor.id
        user_name = activity.actor.display_name

        # TODO

        return True, None, None

    def __call__(self, *args, **kwargs) -> (bool, str):
        if not self.enabled:
            return True, None, None

        data, activity = args[0], args[1]
        try:
            return self._process(data, activity)
        except Exception as e:
            logger.error("could not execute plugin single_session: %s" % str(e))
            logger.exception(traceback.format_exc())
            return (
                False,
                ErrorCodes.VALIDATION_ERROR,
                "could not execute validation plugin single_session",
            )
