class UserNotInGroupException(Exception):
    def __init__(self, message):
        self.message = f"user is not in group: {message}"


class InvalidRangeException(Exception):
    def __init__(self, message):
        self.message = message


class UserIsKickedException(Exception):
    def __init__(self, group_id: str, user_id: int):
        self.message = f"user {user_id} has been kicked from group {group_id}"


class NoSuchGroupException(Exception):
    def __init__(self, message):
        self.message = f"no such group: {message}"


class NoSuchMessageException(Exception):
    def __init__(self, message):
        self.message = f"no such message: {message}"


class NoSuchAttachmentException(Exception):
    def __init__(self, message):
        self.message = f"no such attachment: {message}"


class NoSuchUserException(Exception):
    def __init__(self, message):
        self.message = f"no such user: {message}"


class GroupIsFrozenOrArchivedException(Exception):
    def __init__(self, message):
        self.message = f"group is frozen or archived: {message}"


class QueryValidationError(Exception):
    def __init__(self, message):
        self.message = f"query validation error: {message}"


class UserStatsOrGroupAlreadyCreated(Exception):
    def __init__(self, message):
        self.message = message
