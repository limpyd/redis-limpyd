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
    connection = redis.Redis(
        host=ConnectionSettings.HOST,
        port=ConnectionSettings.PORT,
        db=ConnectionSettings.DB_ID,
    )
    return connection
