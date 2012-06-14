"Idea is to provide an *easy* way to store python objects in Redis, *without losing the power and the control of the Redis API*, in a ''limpid'' way. So, scope is to provide just as abstraction as needed."
VERSION = (0, 0, 1)

__author__ = 'Yohan Bonifacel'
__contact__ = "y.boniface@liberation.fr"
__homepage__ = "https://github.com/liberation/redis-limpyd"
__version__ = ".".join(map(str, VERSION))


import redis


DEFAULT_CONNECTION_SETTINGS = dict(
    host="localhost",
    port=6379,
    db=0
)

TEST_CONNECTION_SETTINGS = DEFAULT_CONNECTION_SETTINGS.copy()
TEST_CONNECTION_SETTINGS['db'] = 15


def redis_connect(settings):
    """
    Connect to redis and cache the new connection
    """
    # compute a unique key for this settings, for caching. Work on the whole
    # dict without directly using known keys to allow the use of unix socket
    # connection or any other (future ?) way to connect to redis
    connection_key = ':'.join([str(settings[k]) for k in sorted(settings)])
    if connection_key not in redis_connect.cache:
        redis_connect.cache[connection_key] = redis.StrictRedis(**settings)
    return redis_connect.cache[connection_key]
redis_connect.cache = {}
