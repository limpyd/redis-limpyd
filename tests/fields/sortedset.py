from limpyd import fields
from limpyd.exceptions import UniquenessError

from ..model import TestRedisModel, BaseModelTest


class SortedSetModel(TestRedisModel):
    field = fields.SortedSetField(indexable=True)


class IndexableSortedSetFieldTest(BaseModelTest):

    model = SortedSetModel

    def test_sortedset_can_be_set_at_init_from_a_dict(self):
        obj = self.model(field={'foo': 1, 'bar': 2})
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([obj._pk], field='bar')

    def test_sortedset_can_be_set_at_init_from_a_list(self):
        obj = self.model(field=[1, 'foo', 2, 'bar'])
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([obj._pk], field='bar')

    def test_indexable_sorted_sets_are_indexed(self):
        obj = self.model()

        # add one value
        obj.field.zadd(1.0, 'foo')
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([], field='bar')

        # add another value
        with self.assertNumCommands(5):
            # check that only 5 commands occured: zadd + index of value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zadd(2.0, 'bar')
        # check collections
        self.assertCollection([obj._pk], field='foo')
        self.assertCollection([obj._pk], field='bar')

        # remove a value
        with self.assertNumCommands(5):
            # check that only 5 commands occured: zrem + deindex of value
            # + 3 for the lock (set at the biginning, check/unset at the end))
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

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for zincr, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zincrby('foo', 5.0)

        # check that the new value is indexed
        self.assertCollection([obj._pk], field='foo')

        # check that the previous value was not deindexed
        self.assertCollection([obj._pk], field='ignorable')

    def test_zremrange_reindex_all_values(self):
        obj = self.model()

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
        self.assertCollection([], field='foo')
        self.assertCollection([], field='bar')
        self.assertCollection([obj._pk], field='baz')

    def test_zremrangebyrank_reindex_all_values(self):
        obj = self.model()

        obj.field.zadd(foo=1, bar=2, baz=3, faz=4)

        # we remove two values
        with self.assertNumCommands(12):
            # check that we had 10 commands:
            # - 1 to get all existing values to deindex
            # - 4 to deindex all values
            # - 1 for the zremrangebyrank
            # - 1 to get all remaining values to index
            # - 2 to index the remaining values
            # - 3 for the lock (set at the biginning, check/unset at the end))
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
        with self.assertNumCommands(7):
            # check that we had 10 commands:
            # - 3 for the lock (set at the biginning, check/unset at the end))
            # - 1 to get all existing values to deindex
            # - 2 to deindex all values
            # - 1 for the delete
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
