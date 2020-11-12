import logging

from dinofw.utils import environ

logger = logging.getLogger(__name__)


class Deleter:
    def __init__(self, env):
        self.env = env

        logger.info("initializing Deleter...")

    def run_deletions(self):
        logger.info("fetching groups with un-deleted messages...")
        session = environ.env.SessionLocal()
        groups = self.env.db.get_groups_with_undeleted_messages(session)

        logger.info(f"about to batch deletions for {len(groups)} groups...")
        pass


app = Deleter(environ.env)
