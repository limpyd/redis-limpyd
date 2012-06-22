import unittest

from limpyd import DEFAULT_CONNECTION_SETTINGS, TEST_CONNECTION_SETTINGS
from limpyd.database import RedisDatabase


class LimpydBaseTest(unittest.TestCase):

    database = RedisDatabase(connection_settings=TEST_CONNECTION_SETTINGS)

    @property
    def connection(self):
        return self.database.connection

    def setUp(self):
        # Ensure that we are on the right DB before flushing
        current_db_id = self.connection.connection_pool.connection_kwargs['db']
        assert current_db_id != DEFAULT_CONNECTION_SETTINGS['db']
        assert current_db_id == TEST_CONNECTION_SETTINGS['db']
        self.connection.flushdb()

    def tearDown(self):
        self.connection.flushdb()
