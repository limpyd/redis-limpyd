from __future__ import unicode_literals

from limpyd import fields
from limpyd.exceptions import UniquenessError

from ..model import TestRedisModel, BaseModelTest


class SetModel(TestRedisModel):
    field = fields.SetField(indexable=True)


class IndexableSetFieldTest(BaseModelTest):

    model = SetModel

    def test_set_can_be_set_at_init(self):
        obj = self.model(field=[1, 2, 3])
        self.assertCollection([obj._pk], field=1)
        self.assertCollection([obj._pk], field=2)
        self.assertCollection([obj._pk], field=3)

    def test_indexable_sets_are_indexed(self):
        obj = self.model()

        # add one value
        obj.field.sadd('foo')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([], field="bar")

        # add another value
        obj.field.sadd('bar')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([obj._pk], field="bar")

        # remove a value
        obj.field.srem('foo')
        self.assertCollection([], field="foo")
        self.assertCollection([obj._pk], field="bar")

        # remove the object
        obj.delete()
        self.assertCollection([], field="foo")
        self.assertCollection([], field="bar")

    def test_spop_command_should_correctly_deindex_one_value(self):
        # spop remove and return a random value from the set, we don't know which one

        obj = self.model()

        values = {'foo', 'bar'}

        obj.field.sadd(*values)

        with self.assertNumCommands(2 + self.COUNT_LOCK_COMMANDS):
            # check that we had only 2 commands: one for spop, one for deindexing the value
            # + n for the lock (set at the beginning, check/unset at the end))
            poped_value = obj.field.spop()

        self.assertIn(poped_value, values)
        values -= {poped_value}
        self.assertSetEqual(obj.field.proxy_get(), values)
        self.assertCollection([obj._pk], field=list(values)[0])
        self.assertCollection([], field=poped_value)

    def test_spop_command_should_correctly_deindex_many_values(self):
        # spop with count remove and return some random values from the set, we don't know which ones

        obj = self.model()

        values = {'foo', 'bar', 'baz', 'qux'}

        obj.field.sadd(*values)

        with self.assertNumCommands(3 + self.COUNT_LOCK_COMMANDS):
            # check that we had only 3 commands: one for spop, two for deindexing the two values
            # + n for the lock (set at the biginning, check/unset at the end))
            poped_values = set(obj.field.spop(2))

        self.assertEqual(len(poped_values), 2)
        self.assertTrue(poped_values.issubset(values))
        values -= poped_values
        self.assertEqual(obj.field.proxy_get(), values)
        # we should have the two left values indexed
        for value in values:
            self.assertCollection([obj._pk], field=value)
        # but both poped ones should not
        for value in poped_values:
            self.assertCollection([], field=value)

        # we can pop more that it exists, it should pop all
        poped_values = set(obj.field.spop(20))
        self.assertEqual(len(poped_values), 2)
        self.assertSetEqual(poped_values, values)
        self.assertEqual(obj.field.proxy_get(), set())
        # removed values should not be indexed anymore
        for value in values:
            self.assertCollection([], field=value)

    def test_delete_set(self):
        obj = self.model()
        obj.field.sadd('foo')
        obj.field.delete()
        self.assertEqual(obj.field.proxy_get(), set([]))


class Crew(TestRedisModel):
    members = fields.SetField(unique=True)


class UniquenessSetFieldTest(BaseModelTest):

    model = Crew

    def test_unique_setfield_should_not_be_settable_twice_at_init(self):
        crew = self.model(members=['Giovanni', 'Paolo'])
        self.assertCollection([crew._pk], members="Giovanni")
        with self.assertRaises(UniquenessError):
            self.model(members=['Giuseppe', 'Giovanni'])
        self.assertCollection([crew._pk], members="Giovanni")
        self.assertCollection([], members="Giuseppe")

    def test_sadd_should_hit_uniqueness_check(self):
        crew1 = self.model(members=['Giovanni', 'Paolo'])
        self.assertCollection([crew1._pk], members="Giovanni")
        crew2 = self.model(members=['Giuseppe', 'Salvatore'])
        with self.assertRaises(UniquenessError):
            crew2.members.sadd('Norberto', 'Giovanni')
        self.assertCollection([crew1._pk], members="Giovanni")
        self.assertCollection([crew2._pk], members="Giuseppe")
        self.assertCollection([crew2._pk], members="Salvatore")
        self.assertCollection([], members="Norberto")


class ScanSetFieldTest(BaseModelTest):

    model = SetModel

    def test_sscan_scan_set_content(self):
        obj = self.model(field={'foo', 'bar', 'baz'})

        self.assertSetEqual(set(obj.field.sscan()), {'foo', 'bar', 'baz'})
        self.assertSetEqual(set(obj.field.sscan(match='ba*')), {'bar', 'baz'})


class SortListFieldTest(BaseModelTest):

    model = SetModel

    def test_sort_should_sort_data(self):
        obj = self.model(field={"foo", "bar", "baz", "faz"})
        self.assertListEqual(
            obj.field.sort(start=0, num=3, alpha=True, desc=True),
            ['foo', 'faz', 'baz']
        )
