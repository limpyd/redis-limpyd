from limpyd import fields

from ..model import TestRedisModel, BaseModelTest


class Vegetable(TestRedisModel):
    name = fields.StringField(indexable=True)
    color = fields.StringField()
    pip = fields.StringField(indexable=True)


class IndexableStringFieldTest(BaseModelTest):

    model = Vegetable

    def test_set_should_be_indexed(self):
        vegetable = self.model()
        vegetable.name.set('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')

        vegetable.name.set('pepper')
        self.assertCollection([], name='aubergine')
        self.assertCollection([vegetable._pk], name='pepper')

    def test_set_should_deindex_before_reindexing(self):
        vegetable = self.model()
        vegetable.name.set('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')

        name = vegetable.name.getset('pepper')
        self.assertEqual(name, 'aubergine')
        self.assertCollection([], name='aubergine')
        self.assertCollection([vegetable._pk], name='pepper')

    def test_delete_should_deindex(self):
        vegetable = self.model()
        vegetable.name.set('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')
        vegetable.name.delete()
        self.assertCollection([], name='aubergine')

    def test_append_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.name.set('sweet')
        self.assertCollection([vegetable._pk], name='sweet')
        vegetable.name.append(' pepper')
        self.assertCollection([], name='sweet')
        self.assertCollection([vegetable._pk], name='sweet pepper')

    def test_decr_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10)
        self.assertCollection([vegetable._pk], pip=10)
        with self.assertNumCommands(7):
            # Check number of queries
            # - 3 for lock
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            vegetable.pip.decr()
        self.assertCollection([], pip=10)
        self.assertCollection([vegetable._pk], pip=9)

    def test_incr_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10)
        self.assertCollection([vegetable._pk], pip=10)
        with self.assertNumCommands(7):
            # Check number of queries
            # - 3 for lock
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            vegetable.pip.incr(3)
        self.assertCollection([], pip=10)
        self.assertCollection([vegetable._pk], pip=13)

    def test_incrbyfloat_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.pip.set(10.3)
        self.assertCollection([vegetable._pk], pip=10.3)
        with self.assertNumCommands(7):
            # Check number of queries
            # - 3 for lock
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            vegetable.pip.incrbyfloat(3.9)
        self.assertCollection([], pip=10.3)
        self.assertCollection([vegetable._pk], pip=14.2)

    def test_setnx_should_index_only_if_value_has_been_set(self):
        vegetable = self.model()
        vegetable.name.setnx('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')
        with self.assertNumCommands(4):
            # Check number of queries
            # - 3 for lock
            # - 1 for setnx
            vegetable.name.setnx('pepper')
        self.assertCollection([], name='pepper')

    def test_setrange_should_deindex_and_reindex(self):
        vegetable = self.model()
        vegetable.name.setnx('aubergine')
        self.assertCollection([vegetable._pk], name='aubergine')
        with self.assertNumCommands(8):
            # Check number of queries
            # - 3 for lock
            # - 2 for deindex (getting value from redis)
            # - 1 for setrange
            # - 2 for reindex (getting value from redis)
            vegetable.name.setrange(2, 'gerb')
        self.assertEqual(vegetable.name.get(), 'augerbine')
        self.assertCollection([], name='aubergine')
        self.assertCollection([vegetable._pk], name='augerbine')
