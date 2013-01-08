# -*- coding:utf-8 -*-

from redis.exceptions import RedisError

from limpyd import fields

from ..base import LimpydBaseTest
from ..model import TestRedisModel


class IndexableListFieldTest(LimpydBaseTest):

    class ListModel(TestRedisModel):
        field = fields.ListField(indexable=True)

    def test_indexable_lists_are_indexed(self):
        obj = self.ListModel()

        # add one value
        obj.field.lpush('foo')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

        # add another value
        obj.field.lpush('bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        # remove a value
        obj.field.rpop()  # will remove foo
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        obj.delete()
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

        # test we can add many values at the same time
        obj = self.ListModel()
        obj.field.rpush('foo', 'bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

    def test_pop_commands_should_correctly_deindex_one_value(self):
        obj = self.ListModel()

        obj.field.lpush('foo', 'bar')

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for lpop, one for deindexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            bar = obj.field.lpop()

        self.assertEqual(bar, 'bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

    def test_pushx_commands_should_correctly_index_only_its_values(self):
        obj = self.ListModel()

        # check that pushx on an empty list does nothing
        obj.field.lpushx('foo')
        self.assertEqual(obj.field.proxy_get(), [])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())

        # add a value to really test pushx
        obj.field.lpush('foo')
        # then test pushx
        with self.assertNumCommands(5):
            # check that we had only 5 comands, one for the rpushx, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.rpushx('bar')

        # test list and collection, to be sure
        self.assertEqual(obj.field.proxy_get(), ['foo', 'bar'])
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

    def test_lrem_command_should_correctly_deindex_only_its_value_when_possible(self):
        obj = self.ListModel()

        obj.field.lpush('foo', 'bar', 'foo',)

        #remove all foo
        with self.assertNumCommands(5):
            # check that we had only 5 comands, one for the lrem, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.lrem(0, 'foo')

        # no more foo in the list
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        # add more foos to test lrem with another count parameter
        obj.field.lpush('foo')
        obj.field.rpush('foo')

        # remove foo at the start
        with self.assertNumCommands(11):
            # we did a lot of calls to reindex, just check this:
            # - 1 lrange to get all values before the lrem
            # - 3 srem to deindex the 3 values (even if two values are the same)
            # - 1 lrem call
            # - 1 lrange to get all values after the rem
            # - 2 sadd to index the two remaining values
            # - 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.lrem(1, 'foo')

        # still a foo in the list
        self.assertEqual(obj.field.proxy_get(), ['bar', 'foo'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))

    def test_lset_command_should_correctly_deindex_and_index_its_value(self):
        obj = self.ListModel()

        obj.field.lpush('foo')

        # replace foo with bar
        with self.assertNumCommands(7):
            # we should have 7 calls:
            # - 1 lindex to get the current value
            # - 1 to deindex this value
            # - 1 for the lset call
            # - 1 to index the new value
            # - 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.lset(0, 'bar')

        # check collections
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        # replace an inexisting value will raise, without (de)indexing anything)
        with self.assertNumCommands(5):
            # we should have 5 calls:
            # - 1 lindex to get the current value, which is None (out f range) so
            #   nothing to deindex
            # - 1 for the lset call
            # + 3 for the lock (set at the biginning, check/unset at the end))
            with self.assertRaises(RedisError):
                obj.field.lset(1, 'baz')

        # check collections are not modified
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='baz')), set())

    def test_linsert_should_only_index_its_value(self):

        obj = self.ListModel()

        obj.field.lpush('foo')
        self.assertEqual(obj.field.proxy_get(), ['foo'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))

        nb_key_before = len(self.connection.keys())
        obj.field.linsert('before', 'foo', 'thevalue')
        # It should only have add one key for the new index
        nb_key_after = len(self.connection.keys())
        self.assertEqual(nb_key_after, nb_key_before + 1)

        self.assertEqual(obj.field.proxy_get(), ['thevalue', 'foo'])
        # Foo may still be indexed
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='thevalue')), set([obj._pk]))
