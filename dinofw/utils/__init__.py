import logging
from base64 import b64decode
from base64 import b64encode

from dinofw import environ
from dinofw.config import SessionKeys
from dinofw.utils.exceptions import UserExistsException

logger = logging.getLogger(__name__)


def b64d(s: str) -> str:
    if s is None:
        return ""

    s = s.strip()
    if len(s) == 0:
        return ""

    try:
        return str(b64decode(bytes(s, "utf-8")), "utf-8")
    except Exception as e:
        logger.error(f"could not b64decode because: {str(e)}, value was: {str(s)}")
    return ""


def b64e(s: str) -> str:
    if s is None:
        return ""

    s = s.strip()
    if len(s) == 0:
        return ""

    try:
        return str(b64encode(bytes(s, "utf-8")), "utf-8")
    except Exception as e:
        logger.error(f"could not b64encode because: {str(e)}, value was: {str(s)}")
    return ""


def get_user_info_attachments_for(user_id: str, encode_atts: bool = True, include_ua: bool = False) -> list:
    attachments = list()
    for info_key, info_val in environ.env.auth.get_user_info(user_id).items():
        attachments.append({
            'objectType': info_key,
            'content': b64e(info_val) if encode_atts else info_val
        })

    if include_ua:
        for key in SessionKeys.user_agent_keys.value:
            agent_value = environ.env.session.get(key)
            attachments.append({
                'objectType': key,
                'content': b64e(agent_value) if encode_atts else agent_value
            })

    return attachments


def get_user_status(user_id: str, skip_cache: bool = False) -> str:
    return str(environ.env.db.get_user_status(user_id, skip_cache))


def create_or_update_user(user_id: str, user_name: str) -> None:
    try:
        environ.env.db.create_user(user_id, user_name)
    except UserExistsException:
        pass

    environ.env.db.set_user_name(user_id, user_name)

