import unittest

from limpyd import ConnectionSettings, TESTS_DB_ID, PROD_DB_ID, get_connection

class LimpydBaseTest(unittest.TestCase):

    def setUp(self):
        # FIXME: is it tread safe?
        ConnectionSettings.DB_ID = TESTS_DB_ID
        self.connection = get_connection()
        # Ensure that we are on the right DB before flushing
        current_db_id = self.connection.connection_pool.connection_kwargs['db']
        assert current_db_id != PROD_DB_ID
        assert current_db_id == TESTS_DB_ID
        self.connection.flushdb()
    
    def tearDown(self):
        self.connection.flushdb()

