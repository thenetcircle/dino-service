from dinofw.cache import ICache


class CacheRedis(ICache):
    def __init__(self):
        pass

    def always_returns_none(self, *args, **kwargs):
        return None

    def __getattr__(self, item):
        return self.always_returns_none
