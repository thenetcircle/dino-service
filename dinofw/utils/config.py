from typing import Final


class GroupTypes:
    GROUP: Final = 0
    ONE_TO_ONE: Final = 1


class MessageTypes:
    MESSAGE: Final = 0
    NO_THANKS: Final = 1
    NO_THANKS_HIDE: Final = 2
    IMAGE: Final = 3
    GREETER_MEETER_AUTO: Final = 4
    GREETER_MEETER_MANUAL: Final = 5

    ACTION: Final = 100


class PayloadStatus:
    DELETED: Final = -2
    ERROR: Final = -1
    PENDING: Final = 0
    RESIZED: Final = 1


class DefaultValues:
    PER_PAGE: Final = 100


class EventTypes:
    JOIN = "join"
    LEAVE = "leave"
    GROUP = "group"
    READ = "read"
    EDIT = "edit"
    ATTACHMENT = "attachment"
    MESSAGE = "message"
    ACTION_LOG = "action_log"
    DELETE_ATTACHMENT = "delete_attachment"
    DELETE_MESSAGE = "delete_message"


class RedisKeys:
    RKEY_AUTH: Final = "user:auth:{}"  # user:auth:user_id
    RKEY_USERS_IN_GROUP: Final = "group:users:{}"  # group:users:group_id
    RKEY_LAST_SEND_TIME: Final = "group:lastsent:{}"  # group:lastsent:group_id
    RKEY_LAST_READ_TIME: Final = "group:lastread:{}"  # group:lastread:group_id
    RKEY_LAST_READ_TIME_OLDEST: Final = "group:lastread:oldest:{}"  # group:lastread:oldest:group_id
    RKEY_USER_STATS_IN_GROUP: Final = "group:stats:{}"  # group:stats:group_id
    RKEY_UNREAD_IN_GROUP: Final = "group:unread:{}"  # user:unread:group_id
    RKEY_HIDE_GROUP: Final = "group:hide:{}"  # group:hide:group_id
    RKEY_MESSAGES_IN_GROUP: Final = "group:messages:{}"  # group:messages:group_id
    RKEY_GROUP_COUNT_INCL_HIDDEN: Final = "group:count:inclhidden:{}"  # group:count:inclhidden:user_id
    RKEY_GROUP_COUNT_NO_HIDDEN: Final = "group:count:visible:{}"  # group:count:visible:user_id
    RKEY_SENT_MSGS_COUNT_IN_GROUP = "group:count:sent:{}"  # group:count:sent:group_id
    RKEY_LAST_SENT_TIME_USER: Final = "user:lastsent:{}"  # user:lastsent:user_id
    RKEY_LAST_READ_TIME_USER: Final = "user:lastread:{}"  # user:lastread:user_id
    RKEY_LAST_MESSAGE_TIME: Final = "group:lastmsgtime:{}"  # group:lastmsgtime:group_id
    RKEY_GROUP_EXISTS: Final = "group:exist:{}"  # group:exist:group_id
    RKEY_ATT_COUNT_GROUP_USER: Final = "att:count:user:{}:{}"  # att:count:user:group_id:user_id
    RKEY_DELETE_BEFORE: Final = "delete:before:user:{}:{}"  # delete:before:user:group_id:user_id
    RKEY_TOTAL_UNREAD_COUNT = "unread:msgs:{}"  # unread:msgs:user_id
    RKEY_UNREAD_GROUPS = "unread:groups:{}"  # unread:groups:user_id
    RKEY_CLIENT_ID = "user:{}:{}:clientids"  # user:domain:user_id:clientids

    @staticmethod
    def client_id(domain: str, user_id: int) -> str:
        return RedisKeys.RKEY_CLIENT_ID.format(domain, user_id)

    @staticmethod
    def total_unread_count(user_id: int) -> str:
        return RedisKeys.RKEY_TOTAL_UNREAD_COUNT.format(user_id)

    @staticmethod
    def unread_groups(user_id: int) -> str:
        return RedisKeys.RKEY_UNREAD_GROUPS.format(user_id)

    @staticmethod
    def delete_before(group_id: str, user_id: int) -> str:
        return RedisKeys.RKEY_DELETE_BEFORE.format(group_id, user_id)

    @staticmethod
    def attachment_count_group_user(group_id: str, user_id: int) -> str:
        return RedisKeys.RKEY_ATT_COUNT_GROUP_USER.format(group_id, user_id)

    @staticmethod
    def sent_message_count_in_group(group_id: str) -> str:
        return RedisKeys.RKEY_SENT_MSGS_COUNT_IN_GROUP.format(group_id)

    @staticmethod
    def group_exists(group_id: str) -> str:
        return RedisKeys.RKEY_GROUP_EXISTS.format(group_id)

    @staticmethod
    def last_message_time(group_id: str) -> str:
        return RedisKeys.RKEY_LAST_MESSAGE_TIME.format(group_id)

    @staticmethod
    def last_read_time_user(user_id: int) -> str:
        return RedisKeys.RKEY_LAST_READ_TIME_USER.format(user_id)

    @staticmethod
    def last_sent_time_user(user_id: int) -> str:
        return RedisKeys.RKEY_LAST_SENT_TIME_USER.format(user_id)

    @staticmethod
    def count_group_types_including_hidden(user_id: int) -> str:
        return RedisKeys.RKEY_GROUP_COUNT_INCL_HIDDEN.format(user_id)

    @staticmethod
    def count_group_types_not_including_hidden(user_id: int) -> str:
        return RedisKeys.RKEY_GROUP_COUNT_NO_HIDDEN.format(user_id)

    @staticmethod
    def messages_in_group(group_id: str) -> str:
        return RedisKeys.RKEY_MESSAGES_IN_GROUP.format(group_id)

    @staticmethod
    def hide_group(group_id: str) -> str:
        return RedisKeys.RKEY_HIDE_GROUP.format(group_id)

    @staticmethod
    def user_stats_in_group(group_id: str) -> str:
        return RedisKeys.RKEY_USER_STATS_IN_GROUP.format(group_id)

    @staticmethod
    def oldest_last_read_time(group_id: str):
        return RedisKeys.RKEY_LAST_READ_TIME_OLDEST.format(group_id)

    @staticmethod
    def last_read_time(group_id: str) -> str:
        return RedisKeys.RKEY_LAST_READ_TIME.format(group_id)

    @staticmethod
    def last_send_time(group_id: str) -> str:
        return RedisKeys.RKEY_LAST_SEND_TIME.format(group_id)

    @staticmethod
    def user_in_group(group_id: str) -> str:
        return RedisKeys.RKEY_USERS_IN_GROUP.format(group_id)

    @staticmethod
    def unread_in_group(group_id: str) -> str:
        return RedisKeys.RKEY_UNREAD_IN_GROUP.format(group_id)

    @staticmethod
    def auth_key(user_id: str) -> str:
        return RedisKeys.RKEY_AUTH.format(user_id)


class ConfigKeys:
    LOG_LEVEL = "log_level"
    LOG_FORMAT = "log_format"
    LOGGING = "logging"
    DATE_FORMAT = "date_format"
    DEBUG = "debug"
    TESTING = "testing"
    STORAGE = "storage"
    KEY_SPACE = "key_space"
    CACHE_SERVICE = "cache"
    PUBLISHER = "publisher"
    STATS_SERVICE = "stats"
    KAFKA = "kafka"
    TOPIC = "topic"
    MQTT = "mqtt"
    MQTT_AUTH = "mqtt_auth"
    HOST = "host"
    TYPE = "type"
    TTL = "ttl"
    STRATEGY = "strategy"
    REPLICATION = "replication"
    DSN = "dsn"
    DATABASE = "database"
    DB = "db"
    PORT = "port"
    USER = "user"
    PASSWORD = "password"
    PREFIX = "prefix"
    INCLUDE_HOST_NAME = "include_hostname"
    URI = "uri"
    DROPPED_EVENT_FILE = "dropped_log"
    TRACE_SAMPLE_RATE = "trace_sample_rate"
    POOL_SIZE = "pool_size"
    MAX_CLIENT_IDS = "max_client_ids"

    # will be overwritten even if specified in config file
    ENVIRONMENT = "_environment"
    VERSION = "_version"
    LOGGER = "_logger"
    REDIS = "_redis"
    SESSION = "_session"
    ACL = "_acl"

    DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)-18s - %(levelname)-7s - %(message)s"
    DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
    DEFAULT_LOG_LEVEL = "INFO"


class ErrorCodes(object):
    OK = 200
    UNKNOWN_ERROR = 250
    USER_NOT_IN_GROUP = 600
    NO_SUCH_GROUP = 601
    NO_SUCH_MESSAGE = 602
    NO_SUCH_ATTACHMENT = 603
    NO_SUCH_USER = 604
    WRONG_PARAMETERS = 605
