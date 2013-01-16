from redis.exceptions import DataError

from limpyd import fields
from limpyd.exceptions import UniquenessError

from ..base import LimpydBaseTest
from ..model import TestRedisModel


class HMTest(LimpydBaseTest):
    """
    Test behavior of hmset and hmget
    """

    class HMTestModel(TestRedisModel):
        foo = fields.InstanceHashField()
        bar = fields.InstanceHashField(indexable=True)
        baz = fields.InstanceHashField(unique=True)

    def test_hmset_should_set_all_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        self.assertEqual(obj.foo.hget(), 'FOO')
        self.assertEqual(obj.bar.hget(), 'BAR')
        self.assertEqual(obj.baz.hget(), 'BAZ')

    def test_hmget_should_get_all_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        data = obj.hmget('foo', 'bar', 'baz')
        self.assertEqual(data, ['FOO', 'BAR', 'BAZ'])

    def test_empty_hmget_call_should_return_nothing(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        data = obj.hmget()
        self.assertEqual(data, [])

    def test_hmset_should_index_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        self.assertEqual(set(self.HMTestModel.collection(bar='BAR')), set([obj._pk]))
        self.assertEqual(set(self.HMTestModel.collection(baz='BAZ')), set([obj._pk]))

    def test_hmset_should_clear_cache_for_fields(self):
        obj = self.HMTestModel()
        obj.foo.hget()  # set the cache
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        hits_before = self.connection.info()['keyspace_hits']
        obj.foo.hget()  # should miss the cache and hit redis
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(hits_before + 1, hits_after)

    def test_hmset_should_not_index_if_an_error_occurs(self):
        self.HMTestModel(baz="BAZ")
        test_obj = self.HMTestModel()
        with self.assertRaises(UniquenessError):
            # The order of parameters below is important. Yes all are passed via
            # the kwargs dict, but order is not random, it's consistent, and
            # here i have to be sure that "bar" is managed first in hmset, so i
            # do some tests to always have the wanted order.
            # So bar will be indexed, then baz will raise because we already
            # set the "BAZ" value for this field.
            test_obj.hmset(baz='BAZ', foo='FOO', bar='BAR')
        # We must not have an entry in the bar index with the BAR value because
        # the hmset must have raise an exception and revert index already set.
        self.assertEqual(set(self.HMTestModel.collection(bar='BAR')), set())

    def test_hmget_should_get_values_from_cache(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        # fill the cache
        obj.foo.hget()

        # get it from cache
        hits_before = self.connection.info()['keyspace_hits']
        obj.hmget('foo')
        hits_after = self.connection.info()['keyspace_hits']
        # hmget should not have hit redis
        self.assertEqual(hits_before, hits_after)

        # get one from cache, one from redis
        hits_before = self.connection.info()['keyspace_hits']
        obj.hmget('foo', 'bar')
        hits_after = self.connection.info()['keyspace_hits']
        # hmget should have hit redis to get bar
        self.assertEqual(hits_before + 1, hits_after)

    def test_hmget_should_cache_retrieved_values_for_hget(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR', baz='BAZ')
        obj.hmget('foo', 'bar')
        with self.assertNumCommands(0):
            foo = obj.foo.hget()
            self.assertEqual(foo, 'FOO')

    def test_hmget_result_is_not_cached_itself(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        obj.hmget('foo', 'bar')
        obj.foo.hset('FOO2')
        with self.assertNumCommands(1):
            data = obj.hmget('foo', 'bar')
            self.assertEqual(data, ['FOO2', 'BAR'])
