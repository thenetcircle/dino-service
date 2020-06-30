import logging
import random
from datetime import datetime
import pytz
from typing import List
from uuid import uuid4 as uuid

from dinofw.rest.models import Message, ActionLog
from dinofw.rest.models import HistoryQuery
from dinofw.rest.models import Histories

logger = logging.getLogger(__name__)


class GroupResource:
    async def messages(self, group_id: str, query: HistoryQuery) -> List[Message]:
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = int(float(now.strftime("%s")))

        message = Message(
            message_id=str(uuid()),
            group_id=group_id,
            user_id=int(random.random() * 1000000),
            created_at=now,
            status=0,
            message_type=0,
            read_at=now,
            updated_at=now,
            last_action_log_id=0,
            removed_at=now,
            removed_by_user=0,
            message_payload="some message payload"
        )

        return [message]

    async def histories(self, group_id: str, user_id: int, query: HistoryQuery) -> List[Histories]:
        now = datetime.utcnow()
        now = now.replace(tzinfo=pytz.UTC)
        now = int(float(now.strftime("%s")))

        message = Message(
            message_id=str(uuid()),
            group_id=group_id,
            user_id=int(random.random() * 1000000),
            created_at=now,
            status=0,
            message_type=0,
            read_at=now,
            updated_at=now,
            last_action_log_id=0,
            removed_at=now,
            removed_by_user=0,
            message_payload="some message payload"
        )

        action_log = ActionLog(
            action_id=str(uuid()),
            user_id=int(random.random() * 1000000),
            group_id=group_id,
            message_id=str(uuid()),
            action_type=0,
            created_at=now,
            admin_id=0
        )

        histories = [
            Histories(message=message),
            Histories(action_log=action_log)
        ]

        return histories
