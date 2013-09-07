# -*- coding:utf-8 -*-

from limpyd import fields
from limpyd.exceptions import UniquenessError

from ..model import TestRedisModel, BaseModelTest


class Vegetable(TestRedisModel):
    name = fields.StringField(indexable=True)
    color = fields.StringField()
    pip = fields.StringField(indexable=True)


class StringFieldTest(BaseModelTest):

    model = Vegetable

    def test_set_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.set('plum')

    def test_setnx_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.set('plum')
        # Try again now that is set
        with self.assertNumCommands(1):
            vegetable.color.set('plum')

    def test_setrange_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine", color="dark green")
        with self.assertNumCommands(1):
            vegetable.color.setrange(5, 'blue')
        self.assertEqual(vegetable.color.get(), "dark bluen")

    def test_delete_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        vegetable.color.set('plum')
        with self.assertNumCommands(1):
            vegetable.color.delete()

    def test_incr_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.incr()

    def test_incrbyfloat_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.incrbyfloat("1.5")

    def test_decr_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.decr()

    def test_getset_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine", color="green")
        with self.assertNumCommands(1):
            color = vegetable.color.getset("plum")
        self.assertEqual(color, "green")

    def test_append_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine", color="dark")
        with self.assertNumCommands(1):
            vegetable.color.append(" green")
        self.assertEqual(vegetable.color.get(), "dark green")

    def test_setbit_should_not_make_index_calls(self):
        vegetable = self.model(name="aubergine")
        with self.assertNumCommands(1):
            vegetable.color.setbit(0, 1)

    def test_delete_string(self):
        vegetable = self.model(name="aubergine")
        vegetable.name.delete()
        self.assertEqual(vegetable.name.get(), None)


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
        vegetable.pip.set("10.3")
        self.assertCollection([vegetable._pk], pip="10.3")
        with self.assertNumCommands(7):
            # Check number of queries
            # - 3 for lock
            # - 2 for getting old value and deindexing it
            # - 1 for decr
            # - 1 for reindex
            vegetable.pip.incrbyfloat("3.9")
        self.assertCollection([], pip="10.3")
        self.assertCollection([vegetable._pk], pip="14.2")

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

    def test_setbit_should_deindex_and_reindex(self):
        vegetable = self.model(name="aubergine", pip='@')  # @ = 0b01000000
        with self.assertNumCommands(8):
            # Check number of queries
            # - 3 for lock
            # - 2 for deindex (getting value from redis)
            # - 1 for setbit
            # - 2 for reindex (getting value from redis)
            vegetable.pip.setbit(3, 1)  # 01010000 => P
        self.assertEqual(vegetable.pip.get(), 'P')
        self.assertCollection([], pip='@')
        self.assertCollection([vegetable._pk], pip='P')


class Ferry(TestRedisModel):
    name = fields.StringField(unique=True)


class UniqueStringFieldTest(BaseModelTest):

    model = Ferry

    def test_unique_stringfield_should_not_be_settable_twice_at_init(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        with self.assertRaises(UniquenessError):
            self.model(name=u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")

    def test_set_should_hit_uniqueness_check(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        ferry2 = self.model(name=u"Danièle Casanova")
        with self.assertRaises(UniquenessError):
            ferry2.name.set(u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        self.assertCollection([ferry2._pk], name=u"Danièle Casanova")

    def test_getset_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        ferry2 = self.model(name=u"Danièle Casanova")
        with self.assertRaises(UniquenessError):
            ferry2.name.getset(u"Napoléon Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        self.assertCollection([ferry2._pk], name=u"Danièle Casanova")

    def test_append_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=u"Napoléon Bonaparte")
        ferry2 = self.model(name=u"Napoléon")
        with self.assertRaises(UniquenessError):
            ferry2.name.append(u" Bonaparte")
        self.assertCollection([ferry1._pk], name=u"Napoléon Bonaparte")
        self.assertCollection([ferry2._pk], name=u"Napoléon")

    def test_decr_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=1)
        ferry2 = self.model(name=2)
        with self.assertRaises(UniquenessError):
            ferry2.name.decr()
        self.assertCollection([ferry1._pk], name=1)
        self.assertCollection([ferry2._pk], name=2)

    def test_incr_should_hit_uniqueness_test(self):
        ferry1 = self.model(name=2)
        ferry2 = self.model(name=1)
        with self.assertRaises(UniquenessError):
            ferry2.name.incr()
        self.assertCollection([ferry1._pk], name=2)
        self.assertCollection([ferry2._pk], name=1)

    def test_incrbyfloat_should_hit_uniqueness_test(self):
        ferry1 = self.model(name="3.0")
        ferry2 = self.model(name="2.5")
        with self.assertRaises(UniquenessError):
            ferry2.name.incrbyfloat("0.5")
        self.assertCollection([ferry1._pk], name="3.0")
        self.assertCollection([ferry2._pk], name="2.5")

    def test_setrange_should_hit_uniqueness_test(self):
        ferry1 = self.model(name="Kalliste")
        ferry2 = self.model(name="Kammiste")
        with self.assertRaises(UniquenessError):
            ferry2.name.setrange(2, "ll")
        self.assertCollection([ferry1._pk], name="Kalliste")
        self.assertCollection([ferry2._pk], name="Kammiste")

    def test_setbit_should_hit_uniqueness_test(self):
        ferry1 = self.model(name='P')  # 0b01010000
        ferry2 = self.model(name='@')  # 0b01000000
        with self.assertRaises(UniquenessError):
            ferry2.name.setbit(3, 1)
        self.assertCollection([ferry1._pk], name='P')
        self.assertCollection([ferry2._pk], name='@')
