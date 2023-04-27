class UserNotInGroupException(Exception):
    def __init__(self, message):
        self.message = f"user is not in group: {message}"


class InvalidRangeException(Exception):
    def __init__(self, message):
        self.message = message


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


class QueryValidationError(Exception):
    def __init__(self, message):
        self.message = f"query validation error: {message}"
