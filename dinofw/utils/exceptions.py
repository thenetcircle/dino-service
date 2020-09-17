class UserNotInGroupException(Exception):
    def __init__(self, message):
        self.message = f"user is not in group: {message}"


class NoSuchGroupException(Exception):
    def __init__(self, message):
        self.message = f"no such group: {message}"


class NoSuchMessageException(Exception):
    def __init__(self, message):
        self.message = f"no such message: {message}"


class NoSuchAttachmentException(Exception):
    def __init__(self, message):
        self.message = f"no such attachment: {message}"
