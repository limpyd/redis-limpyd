from limpyd import fields

from ..base import LimpydBaseTest
from ..model import TestRedisModel


class IndexableSortedSetFieldTest(LimpydBaseTest):

    class SortedSetModel(TestRedisModel):
        field = fields.SortedSetField(indexable=True)

    def test_indexable_sorted_sets_are_indexed(self):
        obj = self.SortedSetModel()

        # add one value
        obj.field.zadd(1.0, 'foo')
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())

        # add another value
        with self.assertNumCommands(5):
            # check that only 5 commands occured: zadd + index of value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zadd(2.0, 'bar')
        # check collections
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set([obj._pk]))

        # remove a value
        with self.assertNumCommands(5):
            # check that only 5 commands occured: zrem + deindex of value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zrem('foo')
        # check collections
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set([obj._pk]))

        # remove the object
        obj.delete()
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())

    def test_zincr_should_correctly_index_only_its_own_value(self):
        obj = self.SortedSetModel()

        # add a value, to check that its index is not updated
        obj.field.zadd(ignorable=1)

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for zincr, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zincrby('foo', 5.0)

        # check that the new value is indexed
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))

        # check that the previous value was not deindexed
        self.assertEqual(set(self.SortedSetModel.collection(field='ignorable')), set([obj._pk]))

    def test_zremrange_reindex_all_values(self):
        obj = self.SortedSetModel()

        obj.field.zadd(foo=1, bar=2, baz=3)

        # we remove two values
        with self.assertNumCommands(10):
            # check that we had 10 commands:
            # - 1 to get all existing values to deindex
            # - 3 to deindex all values
            # - 1 for the zremrange
            # - 1 to get all remaining values to index
            # - 1 to index the only remaining value
            # - 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zremrangebyscore(1, 2)

        # check that all values are correctly indexed/deindexed
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='baz')), set([obj._pk]))
