"Idea is to provide an *easy* way to store python objects in Redis, *without losing the power and the control of the Redis API*, in a ''limpid'' way. So, scope is to provide just as abstraction as needed."
VERSION = (0, 0, 1)

__author__ = 'Yohan Bonifacel'
__contact__ = "y.boniface@liberation.fr"
__homepage__ = "https://github.com/liberation/redis-limpyd"
__version__ = ".".join(map(str, VERSION))


import redis

# TODO Make it customisable
PROD_DB_ID = 0
PROD_HOST = "localhost"
PROD_PORT = 6379
TESTS_DB_ID = 15


class ConnectionSettings(object):
    HOST = PROD_HOST
    PORT = PROD_PORT
    DB_ID = PROD_DB_ID


def get_connection():
    connection = redis.StrictRedis(
        host=ConnectionSettings.HOST,
        port=ConnectionSettings.PORT,
        db=ConnectionSettings.DB_ID,
    )
    return connection
