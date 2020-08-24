import logging
from abc import ABC, abstractmethod
from base64 import b64decode
from base64 import b64encode
from typing import List

from dinofw.db.storage.schemas import MessageBase

logger = logging.getLogger(__name__)

