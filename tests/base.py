# -*- coding:utf-8 -*-
from __future__ import unicode_literals
from future.builtins import object

from contextlib import contextmanager
import sys
import unittest

from redis import VERSION as redispy_version, Redis

from limpyd.database import (RedisDatabase, DEFAULT_CONNECTION_SETTINGS)


TEST_CONNECTION_SETTINGS = DEFAULT_CONNECTION_SETTINGS.copy()
TEST_CONNECTION_SETTINGS['db'] = 15

test_database = RedisDatabase(**TEST_CONNECTION_SETTINGS)


class LimpydBaseTest(unittest.TestCase):

    COUNT_LOCK_COMMANDS = 3
    if redispy_version >= (2, 10, 0):
        COUNT_LOCK_COMMANDS = 6

    database = test_database

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

    def count_commands(self):
        """
        Helper method to only count redis commands that work on keys (ie ignore
        commands like info...)
        """
        return self.connection.info()['total_commands_processed']

    def count_keys(self):
        """
        Helper method to return the number of keys in the test database
        """
        return self.connection.dbsize()

    def assertNumCommands(self, num, func=None, *args, **kwargs):
        """
        A context assert, to use with "with":
            with self.assertNumCommands(2):
                obj.field.set(1)
                obj.field.get()
        """
        context = _AssertNumCommandsContext(self, num)
        if func is None:
            return context

        # Basically emulate the `with` statement here.

        context.__enter__()
        try:
            func(*args, **kwargs)
        except:
            context.__exit__(*sys.exc_info())
            raise
        else:
            context.__exit__(*sys.exc_info())

    if not hasattr(unittest.TestCase, 'subTest'):

        @contextmanager
        def subTest(self, msg=None, **params):
            # support for the `subTest` command not available before python 3.4
            # does nothing except running included test
            yield

    def assertSlicingIsCorrect(self, collection, check_data, check_only_length=False, limit=5):
        """Test a wide range of slicing of the given collection, compared to a python list

        Parameters
        ----------
        collection: Collection
            The collection to test. Should not have been sliced yet
        check_data: list
            The python list containing the same values as the limpyd collection.
            The result of slicing the collection will be compared to the result of slicing
            this list
        check_only_length: bool
            Default to ``False``. When ``True``, only the length of the slicing of the collection
            is comparedc to the slicing of the python list. To be used only when resulting content
            cannot be assured (for unsorted collections)
        limit: int
            Default to ``5``, it's the boundary of the slicing ranges that will be tested.
            ``5`` means will use all values from ``-5`` to ``5`` for each of the three parts
            of the slicing.

        """

        # check we have the correct dataset
        if check_only_length:
            assert len(list(collection)) == len(check_data), 'Wrong dataset for this test'
        else:
            assert sorted(collection) == check_data, 'Wrong dataset for this test'

        # do all the slices
        total, optimized = 0, 0
        for start in list(range(-limit, limit+1)) + [None]:
            for stop in list(range(-limit, limit+1)) + [None]:
                for step in range(-limit, limit+1):
                    if not step:
                        continue
                    with self.subTest(Start=start, Stop=stop, step=step):
                        total += 1

                        result = collection[start:stop:step]
                        expected = check_data[start:stop:step]

                        if check_only_length:
                            result = len(result)
                            expected = len(expected)

                        self.assertEqual(
                            result,
                            expected,
                            'Unexpected result for `%s:%s:%s`' % (
                                '' if start is None else start,
                                '' if stop is None else stop,
                                '' if step is None else step,
                            )
                        )
                        if collection._optimized_slicing:
                            optimized += 1

        # ensure we have enough calls that are optimized
        self.assertGreaterEqual(optimized * 100.0 / total, 60,
                                    "Less than 60% slicing resulted in non-optimized calls")


class _AssertNumCommandsContext(object):
    """
    A context to count commands occured
    """
    def __init__(self, test_case, num):
        self.test_case = test_case
        self.num = num

    def __enter__(self):
        self.starting_commands = self.test_case.count_commands()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is not None:
            return

        # we remove 1 to ignore the "info" called in __enter__
        final_commands = self.test_case.count_commands() - 1

        executed = final_commands - self.starting_commands

        self.test_case.assertEqual(
            executed, self.num, "%d commands executed, %d expected" % (
                executed, self.num
            )
        )


skip_if_no_zrangebylex = (
    not hasattr(Redis, 'zrangebylex'),
    'Redis-py %s does not support zrangebylex' % '.'.join(map(str, redispy_version))
)
