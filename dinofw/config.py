class GroupTypes:
    GROUP = 0
    ONE_TO_ONE = 1


class RedisKeys:
    RKEY_AUTH = "user:auth:{}"  # user:auth:user_id
    RKEY_USERS_IN_GROUP = "group:users:{}"  # group:users:group_id
    RKEY_LAST_SEND_TIME = "group:lastsent:{}"  # group:lastsent:group_id
    RKEY_LAST_READ_TIME = "group:lastread:{}"  # group:lastread:group_id
    RKEY_USER_STATS_IN_GROUP = "group:stats:{}"  # group:stats:group_id
    RKEY_UNREAD_IN_GROUP = "group:unread:{}"  # user:unread:group_id
    RKEY_HIDE_GROUP = "group:hide:{}"  # group:hide:group_id

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
    MQTT = "mqtt"
    HOST = "host"
    TYPE = "type"
    # DRIVER = "driver"
    # STREAM = "stream"
    # GROUP = "group"
    TTL = "ttl"
    STRATEGY = "strategy"
    REPLICATION = "replication"
    DSN = "dsn"
    DATABASE = "database"
    # POOL_SIZE = "pool_size"
    DB = "db"
    PORT = "port"
    USER = "user"
    PREFIX = "prefix"
    INCLUDE_HOST_NAME = "include_hostname"
    URI = "uri"

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
