from redis.exceptions import DataError, ResponseError

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
        obj.hmset(baz='QUX')
        data = obj.hmget('bar', 'baz')
        self.assertEqual(data, ['BAR', 'QUX'])

    def test_empty_hmset_call_should_fail(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR', baz='BAZ')
        with self.assertRaises(DataError):
            obj.hmset()
        # nothing modified...
        data = obj.hmget('foo', 'bar', 'baz')
        self.assertEqual(data, ['FOO', 'BAR', 'BAZ'])

    def test_empty_hmget_call_should_failse(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        with self.assertRaises(ResponseError):
            obj.hmget()

    def test_hmset_should_index_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        self.assertEqual(set(self.HMTestModel.collection(bar='BAR')), set([obj._pk]))
        self.assertEqual(set(self.HMTestModel.collection(baz='BAZ')), set([obj._pk]))

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

    def test_hgetall_should_return_all_set_fields(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        data = obj.hgetall()
        self.assertEqual(data, dict(foo='FOO', bar='BAR'))
        obj.foo.hdel()
        data = obj.hgetall()
        self.assertEqual(data, dict(bar='BAR',))

    def test_hkeys_should_return_all_set_fieldnames(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        data = obj.hkeys()
        self.assertEqual(set(data), set(['foo', 'bar']))
        obj.foo.hdel()
        data = obj.hkeys()
        self.assertEqual(set(data), set(['bar', ]))

    def test_hvals_should_return_all_set_values(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        data = obj.hvals()
        self.assertEqual(set(data), set(['FOO', 'BAR']))
        obj.foo.hdel()
        data = obj.hvals()
        self.assertEqual(set(data), set(['BAR', ]))

    def test_hlen_should_return_number_of_set_fields(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        self.assertEqual(obj.hlen(), 2)
        obj.foo.hdel()
        self.assertEqual(obj.hlen(), 1)
