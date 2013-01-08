from limpyd import fields

from ..base import LimpydBaseTest
from ..model import TestRedisModel


class IndexableSetFieldTest(LimpydBaseTest):

    class SetModel(TestRedisModel):
        field = fields.SetField(indexable=True)

    def test_indexable_sets_are_indexed(self):
        obj = self.SetModel()

        # add one value
        obj.field.sadd('foo')
        self.assertEqual(set(self.SetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SetModel.collection(field='bar')), set())

        # add another value
        obj.field.sadd('bar')
        self.assertEqual(set(self.SetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SetModel.collection(field='bar')), set([obj._pk]))

        # remove a value
        obj.field.srem('foo')
        self.assertEqual(set(self.SetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SetModel.collection(field='bar')), set([obj._pk]))

        # remove the object
        obj.delete()
        self.assertEqual(set(self.SetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SetModel.collection(field='bar')), set())

    def test_spop_command_should_correctly_deindex_one_value(self):
        # spop remove and return a random value from the set, we don't know which one

        obj = self.SetModel()

        values = ['foo', 'bar']

        obj.field.sadd(*values)

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for spop, one for deindexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            poped_value = obj.field.spop()

        values.remove(poped_value)
        self.assertEqual(obj.field.proxy_get(), set(values))
        self.assertEqual(set(self.SetModel.collection(field=values[0])), set([obj._pk]))
        self.assertEqual(set(self.SetModel.collection(field=poped_value)), set())
