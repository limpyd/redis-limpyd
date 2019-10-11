from __future__ import unicode_literals

from limpyd import fields
from limpyd.exceptions import LimpydException, UniquenessError

from ..model import TestRedisModel, BaseModelTest


class SortedSetModel(TestRedisModel):
    field = fields.SortedSetField(indexable=True)


class SpecialZaddArgumentsTest(BaseModelTest):

    model = SortedSetModel

    def test_passing_mapping_as_unnamed_arg(self):
        obj = self.model()
        # only the mapping
        count = obj.field.zadd({'foo': 1})
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 1.0),
        ])
        self.assertEqual(count, 1)  # one added
        # if we change, count will be zero (counts only added)
        count = obj.field.zadd({'foo': 2})
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 2.0),
        ])
        self.assertEqual(count, 0)  # zero added
        # if we change and add, count is one
        count = obj.field.zadd({'foo': 2, 'bar': 3})
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 2.0), ('bar', 3.0)
        ])
        self.assertEqual(count, 1)  # one added
        # with ch, count includes changed
        count = obj.field.zadd({'foo': 2.1, 'qux': 4}, True)
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 2.1), ('bar', 3.0), ('qux', 4.0)
        ])
        self.assertEqual(count, 2)  # one added and one changed
        # ch can be passed as kwargs
        count = obj.field.zadd({'foo': 2.2, 'xxx': 5}, ch=True)
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 2.2), ('bar', 3.0), ('qux', 4.0), ('xxx', 5.0)
        ])
        self.assertEqual(count, 2)  # one added and one changed
        # only two args max accepted
        with self.assertRaises(LimpydException):
            obj.field.zadd({'foo': 2.2, 'xxx': 5}, True, False)
        # if ch as kwarg, only one arg accepted
        with self.assertRaises(LimpydException):
            obj.field.zadd({'foo': 2.2, 'xxx': 5}, True, ch=True)
        # only ch is accepted as kwarg
        with self.assertRaises(LimpydException):
            obj.field.zadd({'foo': 2.2, 'xxx': 5}, nx=True)
        # cannot pass mapping as arg and kwarg
        with self.assertRaises(LimpydException):
            obj.field.zadd({'foo': 2.2, 'xxx': 5}, mapping={'x': 1, 'y': 2})

    def test_passing_value_score_as_named_arguments(self):
        obj = self.model()
        # one value
        count = obj.field.zadd(foo=1)
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 1.0),
        ])
        self.assertEqual(count, 1)  # one added
        # many values
        count = obj.field.zadd(foo=1.1, bar=2, baz=3)
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 1.1), ('bar', 2.0), ('baz', 3.0)
        ])
        self.assertEqual(count, 2)  # two added
        # can pass ch
        count = obj.field.zadd(foo=1.2, xxx=4, ch=True)
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 1.2), ('bar', 2.0), ('baz', 3.0), ('xxx', 4.0)
        ])
        self.assertEqual(count, 2)  # one added and one changed
        # but not other flags
        with self.assertRaises(LimpydException):
            obj.field.zadd(foo=1.2, xxx=4, nx=True)
        # it doesn't work if a mapping is passed too
        with self.assertRaises(TypeError):
            obj.field.zadd(mapping={'x': 1, 'y': 2}, foo=1.2, xxx=4)

    def test_passing_mapping_as_kwarg(self):
        obj = self.model()
        # one value
        count = obj.field.zadd(mapping={'foo': 1})
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 1.0),
        ])
        self.assertEqual(count, 1)  # one added
        # can pass ch
        count = obj.field.zadd(mapping={'foo': 1.1, 'bar': 2}, ch=True)
        self.assertEqual(obj.field.zrange(0, -1, withscores=True), [
            ('foo', 1.1), ('bar', 2)
        ])
        self.assertEqual(count, 2)  # one added and one changed
        # but not other flags
        with self.assertRaises(LimpydException):
            obj.field.zadd(mapping={'foo': 1.1, 'bar': 2}, nx=True)

class IndexableSortedSetFieldTest(BaseModelTest):

    model = SortedSetModel

    def test_sortedset_can_be_set_at_init_from_a_dict(self):
        obj = self.model(field={'foo': 1, 'bar': 2})
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([obj._pk], field='bar')

    def test_indexable_sorted_sets_are_indexed(self):
        obj = self.model()

        # add one value
        obj.field.zadd(foo=1.0)
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([], field='bar')

        # add another value
        with self.assertNumCommands(2 + self.COUNT_LOCK_COMMANDS):
            # check that only 2 commands occured: zadd + index of value
            # + n for the lock (set at the biginning, check/unset at the end))
            obj.field.zadd({'bar': 2.0})
        # check collections
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([obj._pk], field='bar')

        # remove a value
        with self.assertNumCommands(2 + self.COUNT_LOCK_COMMANDS):
            # check that only 2 commands occured: zrem + deindex of value
            # + n for the lock (set at the biginning, check/unset at the end))
            obj.field.zrem('foo')
        # check collections
        self.assertCollection([], field='foo')
        self.assertCollection([obj._pk], field='bar')

        # remove the object
        obj.delete()
        self.assertCollection([], field='foo')
        self.assertCollection([], field='bar')

    def test_zincrby_should_correctly_index_only_its_own_value(self):
        obj = self.model()

        # add a value, to check that its index is not updated
        obj.field.zadd(ignorable=1)

        with self.assertNumCommands(2 + self.COUNT_LOCK_COMMANDS):
            # check that we had only 2 commands: one for zincr, one for indexing the value
            # + n for the lock (set at the biginning, check/unset at the end))
            obj.field.zincrby(5.0, 'foo')

        # check new value in sorted set
        self.assertEqual(obj.field.zscore('foo'), 5.0)

        # check that the new value is indexed
        self.assertCollection([obj._pk], field='foo')

        # check that the previous value was not deindexed
        self.assertCollection([obj._pk], field='ignorable')

        # incr again...
        with self.assertNumCommands(2 + self.COUNT_LOCK_COMMANDS):
            obj.field.zincrby(4.4, 'foo')
        self.assertEqual(obj.field.zscore('foo'), 9.4)
        self.assertCollection([obj._pk], field='foo')

    def test_zremrange_reindex_all_values(self):
        obj = self.model()

        obj.field.zadd(foo=1, bar=2, baz=3)

        # we remove two values
        with self.assertNumCommands(7 + self.COUNT_LOCK_COMMANDS):
            # check that we had 7 commands:
            # - 1 to get all existing values to deindex
            # - 3 to deindex all values
            # - 1 for the zremrange
            # - 1 to get all remaining values to index
            # - 1 to index the only remaining value
            # + n for the lock (set at the biginning, check/unset at the end))
            obj.field.zremrangebyscore(1, 2)

        # check that all values are correctly indexed/deindexed
        self.assertCollection([], field='foo')
        self.assertCollection([], field='bar')
        self.assertCollection([obj._pk], field='baz')

    def test_zremrangebyrank_reindex_all_values(self):
        obj = self.model()

        obj.field.zadd(foo=1, bar=2, baz=3, faz=4)

        # we remove two values
        with self.assertNumCommands(9 + self.COUNT_LOCK_COMMANDS):
            # check that we had 9 commands:
            # - 1 to get all existing values to deindex
            # - 4 to deindex all values
            # - 1 for the zremrangebyrank
            # - 1 to get all remaining values to index
            # - 2 to index the remaining values
            # + n for the lock (set at the biginning, check/unset at the end))
            obj.field.zremrangebyrank(1, 2)

        # check that all values are correctly indexed/deindexed
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([], field='bar')
        self.assertCollection([], field='baz')
        self.assertCollection([obj._pk], field='faz')

    def test_delete_should_deindex(self):
        obj = self.model()

        obj.field.zadd(foo=22, bar=34)
        self.assertCollection([obj._pk], field='foo')

        # we remove two values
        with self.assertNumCommands(4 + self.COUNT_LOCK_COMMANDS):
            # check that we had 10 commands:
            # - 1 to get all existing values to deindex
            # - 2 to deindex all values
            # - 1 for the delete
            # + n for the lock (set at the biginning, check/unset at the end))
            obj.field.delete()

        # check that all values are correctly indexed/deindexed
        self.assertCollection([], field='foo')

    def test_delete_set(self):
        obj = self.model()
        obj.field.zadd(foo=22, bar=34)
        obj.field.delete()
        self.assertEqual(obj.field.proxy_get(), [])


class Student(TestRedisModel):
    exams = fields.SortedSetField(unique=True)


class UniquenessSortedSetFieldTest(BaseModelTest):

    model = Student

    def test_unique_sortedsetfield_should_not_be_settable_twice_at_init(self):
        student1 = self.model(exams={"math": 9, "sport": 7})
        self.assertCollection([student1._pk], exams="math")
        with self.assertRaises(UniquenessError):
            self.model(exams={"biology": 8, "sport": 7})
        self.assertCollection([student1._pk], exams="math")
        self.assertCollection([], exams="biology")

    def test_zadd_should_hit_uniqueness_check(self):
        student1 = self.model()
        student1.exams.zadd(math=9, sport=7)
        self.assertCollection([student1._pk], exams="math")
        student2 = self.model(exams={"philosophy": 9})
        with self.assertRaises(UniquenessError):
            student2.exams.zadd(biology=8, sport=7)
        self.assertCollection([student1._pk], exams="math")
        self.assertCollection([], exams="biology")
        self.assertCollection([student2._pk], exams="philosophy")


class ScanSortedSetFieldTest(BaseModelTest):

    model = SortedSetModel

    def test_zscan_should_scan_zset_content(self):
        obj = self.model(field={'foo': 1, 'bar': 2})

        self.assertDictEqual(dict(obj.field.zscan()), {'foo': 1, 'bar': 2})
        self.assertDictEqual(dict(obj.field.zscan('ba*')), {'bar': 2})


class SortSortedSetFieldTest(BaseModelTest):

    model = SortedSetModel

    def test_sort_should_sort_data(self):
        obj = self.model(field={'foo': 1, 'bar': 2, 'baz': 3, 'faz': 4})
        self.assertListEqual(
            obj.field.sort(start=0, num=3, alpha=True, desc=True),
            ['foo', 'faz', 'baz']
        )
