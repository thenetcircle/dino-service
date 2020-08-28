class UserNotInGroupException(Exception):
    def __init__(self, message):
        self.message = message


class NoSuchGroupException(Exception):
    def __init__(self, message):
        self.message = message
