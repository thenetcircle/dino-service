import logging
import sys

from fastapi import FastAPI

from dinofw.utils import environ

logger = logging.getLogger(__name__)


class Deleter:
    def __init__(self, env):
        self.env = env

        logger.info("initializing Deleter...")

    def run_deletions(self):
        # TODO: add timings and report to grafana

        logger.info("fetching groups with un-deleted messages...")
        session = environ.env.SessionLocal()
        groups = self.env.db.get_groups_with_undeleted_messages(session)

        if len(groups) == 0:
            logger.info("no groups with un-deleted messages, exiting!")
            return

        logger.info(f"about to batch delete messages/attachments for {len(groups)} groups...")
        for group_id, delete_before in groups:
            logger.info(f"group {group_id}: delete all messages <= {delete_before}")

            try:
                self.env.storage.delete_messages_in_group_before(group_id, delete_before)
                self.env.storage.delete_attachments_in_group_before(group_id, delete_before)
                self.env.db.update_first_message_time(group_id, delete_before, session)
            except Exception as e:
                logger.error(f"could not delete messages for group {group_id}: {str(e)}")
                logger.exception(e)
                environ.env.capture_exception(sys.exc_info())


deleter = Deleter(environ.env)
app = FastAPI()


@app.delete("/v1/run")
def run_deletions():
    """
    Call periodically to delete old messages.

    First we find potential groups that may have old messages:

    ```sql
            select
                g.group_id,
                min(u.delete_before)
            from
                groups g,
                user_group_stats u
            where
                g.group_id = u.group_id
            group by
                g.group_id
            having
                coalesce(
                    sum(
                        case when u.delete_before <= g.first_message_time then 1
                        else 0 end
                    ),
                0) = 0;
    ```

    Then we remove all Messages and Attachments with `created_at <= min(delete_before)`. Finally
    we update `first_message_time on those groups to `min(delete_before)` for that group.
    """
    deleter.run_deletions()
