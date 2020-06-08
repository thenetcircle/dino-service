from abc import ABC

from dinofw.config import ErrorCodes


class IResponseFormatter(ABC):
    def __call__(self, *args, **kwargs):
        """
        format the response from an api call based on the configured format

        :param args: index 0 is the status_code from ErrorCodes, index 1 is the data/error message
        :param kwargs: not used
        :return: a formatted response
        """


class SimpleResponseFormatter(IResponseFormatter):
    def __init__(self, code_key: str, data_key: str, error_key: str):
        self.code_key = code_key
        self.data_key = data_key
        self.error_key = error_key

    def __call__(self, *args, **kwargs):
        assert len(args) == 2
        status_code, data = args[0], args[1]

        if status_code != ErrorCodes.OK:
            return {self.code_key: status_code, self.error_key: data}
        return {self.code_key: status_code, self.data_key: data}

    def __repr__(self):
        return 'SimpleResponseFormatter<format="{%s: <status code>, %s: <data dict>, %s: <error message>}">' % \
               (self.code_key, self.data_key, self.error_key)
