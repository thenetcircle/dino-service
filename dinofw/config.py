from enum import Enum


class SessionKeys(Enum):
    user_id = "user_id"
    user_name = "user_name"
    age = "age"
    gender = "gender"
    membership = "membership"
    group = "group"
    country = "country"
    city = "city"
    image = "image"
    has_webcam = "has_webcam"
    fake_checked = "fake_checked"
    token = "token"

    avatar = "avatar"
    app_avatar = "app_avatar"
    app_avatar_safe = "app_avatar_safe"
    enabled_safe = "enabled_safe"

    user_agent = "user_agent"
    user_agent_browser = "user_agent_browser"
    user_agent_version = "user_agent_version"
    user_agent_platform = "user_agent_platform"
    user_agent_language = "user_agent_language"

    user_agent_keys = {
        user_agent,
        user_agent_browser,
        user_agent_version,
        user_agent_platform,
        user_agent_language,
    }

    requires_session_keys = {user_id, user_name, token}


class UserKeys:
    STATUS_AVAILABLE = "1"
    STATUS_CHAT = "2"
    STATUS_INVISIBLE = "3"
    STATUS_UNAVAILABLE = "4"
    STATUS_UNKNOWN = "5"


class AckStatus:
    NOT_ACKED = 0
    RECEIVED = 1
    READ = 2


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
    REQ_LOG_LOC = "request_log_location"
    LOG_LEVEL = "log_level"
    LOG_FORMAT = "log_format"
    RESPONSE_FORMAT = "response_format"
    LOGGING = "logging"
    DATE_FORMAT = "date_format"
    DEBUG = "debug"
    QUEUE = "queue"
    EXTERNAL_QUEUE = "ext_queue"
    EXCHANGE = "exchange"
    TESTING = "testing"
    STORAGE = "storage"
    KEY_SPACE = "key_space"
    AUTH_SERVICE = "auth"
    CACHE_SERVICE = "cache"
    STATS_SERVICE = "stats"
    HOST = "host"
    TYPE = "type"
    DRIVER = "driver"
    COORDINATOR = "coordinator"
    STRATEGY = "strategy"
    REPLICATION = "replication"
    DSN = "dsn"
    DATABASE = "database"
    POOL_SIZE = "pool_size"
    DB = "db"
    PORT = "port"
    VHOST = "vhost"
    USER = "user"
    PASSWORD = "password"
    HISTORY = "history"
    LIMIT = "limit"
    PREFIX = "prefix"
    INCLUDE_HOST_NAME = "include_hostname"
    VALIDATION = "validation"
    MAX_MSG_LENGTH = "max_length"
    MAX_USERS_LOW = "max_users_low"
    MAX_USERS_HIGH = "max_users_high"
    MAX_USERS_EXCEPTION = "exception"
    WEB = "web"
    ROOT_URL = "root_url"
    MIN_ROOM_NAME_LENGTH = "min_length"
    MAX_ROOM_NAME_LENGTH = "max_length"
    DISCONNECT_ON_FAILED_LOGIN = "disconnect_on_failed_login"

    ENRICH = "enrich"
    TITLE = "title"
    VERB = "verb"
    HEARTBEAT = "heartbeat"
    TIMEOUT = "timeout"
    INTERVAL = "interval"

    INSECURE = "insecure"
    OAUTH_ENABLED = "oauth_enabled"
    OAUTH_BASE = "base"
    OAUTH_PATH = "path"
    SERVICE_ID = "service_id"
    SERVICE_SECRET = "service_secret"
    AUTH_URL = "authorized_url"
    TOKEN_URL = "token_url"
    CALLBACK_URL = "callback_url"
    UNAUTH_URL = "unauthorized_url"
    USE_FLOATING_MENU = "use_floating_menu"

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
    DEFAULT_REDIS_HOST = "localhost"
    DEFAULT_HISTORY_LIMIT = 500
    DEFAULT_HISTORY_STRATEGY = "top"

    CORS_ORIGINS = "cors_origins"

    ENDPOINT = "endpoint"
    REMOTE = "remote"
    PRIVATE_KEY = "private_key"

    URI = "uri"

    KAFKA = "kafka"
    TOPIC = "topic"


class ErrorCodes(object):
    OK = 200
    UNKNOWN_ERROR = 250

    MISSING_ACTOR_ID = 500
    MISSING_OBJECT_ID = 501
    MISSING_TARGET_ID = 502
    MISSING_OBJECT_URL = 503
    MISSING_TARGET_DISPLAY_NAME = 504
    MISSING_ACTOR_URL = 505
    MISSING_OBJECT_CONTENT = 506
    MISSING_OBJECT = 507
    MISSING_OBJECT_ATTACHMENTS = 508
    MISSING_ATTACHMENT_TYPE = 509
    MISSING_ATTACHMENT_CONTENT = 510
    MISSING_VERB = 511

    INVALID_TARGET_TYPE = 600
    INVALID_ACL_TYPE = 601
    INVALID_ACL_ACTION = 602
    INVALID_ACL_VALUE = 603
    INVALID_STATUS = 604
    INVALID_OBJECT_TYPE = 605
    INVALID_BAN_DURATION = 606
    INVALID_VERB = 607

    EMPTY_MESSAGE = 700
    NOT_BASE64 = 701
    USER_NOT_IN_ROOM = 702
    USER_IS_BANNED = 703
    ROOM_ALREADY_EXISTS = 704
    NOT_ALLOWED = 705
    VALIDATION_ERROR = 706
    ROOM_FULL = 707
    NOT_ONLINE = 708
    TOO_MANY_PRIVATE_ROOMS = 709
    ROOM_NAME_TOO_LONG = 710
    ROOM_NAME_TOO_SHORT = 711
    INVALID_TOKEN = 712
    INVALID_LOGIN = 713
    MSG_TOO_LONG = 714
    MULTIPLE_ROOMS_WITH_NAME = 715
    TOO_MANY_ATTACHMENTS = 716
    NOT_ENABLED = 717
    ROOM_NAME_RESTRICTED = 718
    NOT_ALLOWED_TO_WHISPER_USER = 719
    NOT_ALLOWED_TO_WHISPER_CHANNEL = 720

    NO_SUCH_USER = 800
    NO_SUCH_CHANNEL = 801
    NO_SUCH_ROOM = 802
    NO_ADMIN_ROOM_FOUND = 803
    NO_USER_IN_SESSION = 804
    NO_ADMIN_ONLINE = 805
