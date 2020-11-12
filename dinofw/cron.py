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

        if len(groups) == 0:
            logger.info("no groups with un-deleted messages, exiting!")
            return

        logger.info(f"about to batch delete messages/attachments for {len(groups)} groups...")
        for group_id, delete_before in groups:
            logger.info(f"group {group_id}: delete all messages <= {delete_before}")

            self.env.storage.delete_messages_in_group_before(group_id, delete_before)
            self.env.storage.delete_attachments_in_group_before(group_id, delete_before)

            self.env.db.update_first_message_time(group_id, delete_before, session)


app = Deleter(environ.env)
