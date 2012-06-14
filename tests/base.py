import unittest

from limpyd import DEFAULT_CONNECTION_SETTINGS, TEST_CONNECTION_SETTINGS, redis_connect


class LimpydBaseTest(unittest.TestCase):

    def setUp(self):
        # FIXME: is it tread safe?
        self.connection = redis_connect(TEST_CONNECTION_SETTINGS)
        # Ensure that we are on the right DB before flushing
        current_db_id = self.connection.connection_pool.connection_kwargs['db']
        assert current_db_id != DEFAULT_CONNECTION_SETTINGS['db']
        assert current_db_id == TEST_CONNECTION_SETTINGS['db']
        self.connection.flushdb()

    def tearDown(self):
        self.connection.flushdb()
