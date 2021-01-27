from typing import Final


class GroupTypes:
    GROUP = 0
    ONE_TO_ONE = 1


class MessageTypes:
    MESSAGE = 0
    NO_THANKS = 1
    NO_THANKS_HIDE = 2
    IMAGE = 3
    GREETER_MEETER_AUTO = 4
    GREETER_MEETER_MANUAL = 5

    ACTION = 100


class DefaultValues:
    PER_PAGE: Final = 100

    # TODO: when actions have been defined, use an ActionTypes class or similar
    ACTION_TYPE_JOIN: Final = 0
    ACTION_TYPE_LEAVE: Final = 1


class RedisKeys:
    RKEY_AUTH = "user:auth:{}"  # user:auth:user_id
    RKEY_USERS_IN_GROUP = "group:users:{}"  # group:users:group_id
    RKEY_LAST_SEND_TIME = "group:lastsent:{}"  # group:lastsent:group_id
    RKEY_LAST_READ_TIME = "group:lastread:{}"  # group:lastread:group_id
    RKEY_USER_STATS_IN_GROUP = "group:stats:{}"  # group:stats:group_id
    RKEY_UNREAD_IN_GROUP = "group:unread:{}"  # user:unread:group_id
    RKEY_HIDE_GROUP = "group:hide:{}"  # group:hide:group_id
    RKEY_USER_MESSAGE_STATUS = "user:status:{}"  # user:status:user_id
    RKEY_MESSAGES_IN_GROUP = "group:messages:{}"  # group:messages:group_id
    RKEY_GROUP_COUNT_INCL_HIDDEN = "group:count:inclhidden:{}"  # group:count:inclhidden:user_id
    RKEY_GROUP_COUNT_NO_HIDDEN = "group:count:visible:{}"  # group:count:visible:user_id
    RKEY_LAST_SENT_TIME_USER = "user:lastsent:{}"  # user:lastsent:user_id
    RKEY_LAST_READ_TIME_USER = "user:lastread:{}"  # user:lastread:user_id
    RKEY_LAST_MESSAGE_TIME = "group:lastmsgtime:{}"  # group:lastmsgtime:group_id

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
    def user_message_status(user_id: int) -> str:
        return RedisKeys.RKEY_USER_MESSAGE_STATUS.format(user_id)

    @staticmethod
    def hide_group(group_id: str) -> str:
        return RedisKeys.RKEY_HIDE_GROUP.format(group_id)

    @staticmethod
    def user_stats_in_group(group_id: str) -> str:
        return RedisKeys.RKEY_USER_STATS_IN_GROUP.format(group_id)

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
