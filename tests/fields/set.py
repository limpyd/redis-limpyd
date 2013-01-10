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

        values = ['foo', 'bar']

        obj.field.sadd(*values)

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for spop, one for deindexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            poped_value = obj.field.spop()

        values.remove(poped_value)
        self.assertEqual(obj.field.proxy_get(), set(values))
        self.assertCollection([obj._pk], field=values[0])
        self.assertCollection([], field=poped_value)


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
