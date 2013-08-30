# -*- coding:utf-8 -*-

from redis.exceptions import RedisError

from limpyd import fields
from limpyd.exceptions import UniquenessError

from ..model import TestRedisModel, BaseModelTest


class ListModel(TestRedisModel):
    field = fields.ListField(indexable=True)


class IndexableListFieldTest(BaseModelTest):

    model = ListModel

    def test_listfield_is_settable_at_init(self):
        obj = self.model(field=['foo', 'bar'])
        self.assertEqual(
            obj.field.proxy_get(),
            ['foo', 'bar']
        )
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([obj._pk], field="bar")

    def test_indexable_lists_are_indexed(self):
        obj = self.model()

        # add one value
        obj.field.lpush('foo')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([], field="bar")

        # add another value
        obj.field.lpush('bar')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([obj._pk], field="bar")

        # remove a value
        obj.field.rpop()  # will remove foo
        self.assertCollection([], field="foo")
        self.assertCollection([obj._pk], field="bar")

        obj.delete()
        self.assertCollection([], field="foo")
        self.assertCollection([], field="bar")

        # test we can add many values at the same time
        obj = self.model()
        obj.field.rpush('foo', 'bar')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([obj._pk], field="bar")

    def test_pop_commands_should_correctly_deindex_one_value(self):
        obj = self.model()

        obj.field.lpush('foo', 'bar')

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for lpop, one for deindexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            bar = obj.field.lpop()

        self.assertEqual(bar, 'bar')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([], field="bar")

    def test_pushx_commands_should_correctly_index_only_its_values(self):
        obj = self.model()

        # check that pushx on an empty list does nothing
        obj.field.lpushx('foo')
        self.assertEqual(obj.field.proxy_get(), [])
        self.assertCollection([], field="foo")

        # add a value to really test pushx
        obj.field.lpush('foo')
        # then test pushx
        with self.assertNumCommands(5):
            # check that we had only 5 comands, one for the rpushx, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.rpushx('bar')

        # test list and collection, to be sure
        self.assertEqual(obj.field.proxy_get(), ['foo', 'bar'])
        self.assertCollection([obj._pk], field="bar")

    def test_lrem_command_should_correctly_deindex_only_its_value_when_possible(self):
        obj = self.model()

        obj.field.lpush('foo', 'bar', 'foo',)

        #remove all foo
        with self.assertNumCommands(5):
            # check that we had only 5 comands, one for the lrem, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.lrem(0, 'foo')

        # no more foo in the list
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertCollection([], field="foo")
        self.assertCollection([obj._pk], field="bar")

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
        self.assertCollection([obj._pk], field="foo")

    def test_lset_command_should_correctly_deindex_and_index_its_value(self):
        obj = self.model()

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
        self.assertCollection([], field="foo")
        self.assertCollection([obj._pk], field="bar")

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
        self.assertCollection([obj._pk], field="bar")
        self.assertCollection([], field="baz")

    def test_linsert_should_only_index_its_value(self):
        obj = self.model()

        obj.field.lpush('foo')
        self.assertEqual(obj.field.proxy_get(), ['foo'])
        self.assertCollection([obj._pk], field="foo")

        nb_key_before = len(self.connection.keys())
        obj.field.linsert('before', 'foo', 'thevalue')
        # It should only have add one key for the new index
        nb_key_after = len(self.connection.keys())
        self.assertEqual(nb_key_after, nb_key_before + 1)

        self.assertEqual(obj.field.proxy_get(), ['thevalue', 'foo'])
        # Foo may still be indexed
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([obj._pk], field="thevalue")

    def test_linsert_should_not_be_indexed_if_pivot_is_not_found(self):
        obj = self.model(field=['foo', 'bar'])
        obj.field.linsert('before', 'nonexistingvalue', 'valuetoinsert')
        self.assertCollection([obj._pk], field="foo")
        self.assertCollection([obj._pk], field="bar")
        self.assertCollection([], field="valuetoinsert")

    def test_ltrim_should_deindex_and_reindex(self):
        obj = self.model()
        obj.field.rpush("foo", "bar", "baz", "faz")
        with self.assertNumCommands(12):
            # 3 for lock
            # 1 for getting all values to deindex
            # 4 for deindexing all values
            # 1 for command
            # 1 for getting remaining values
            # 2 for indexing remaining values
            obj.field.ltrim(1, 2)  # keep bar and baz, remove others
        self.assertEqual(obj.field.proxy_get(), ["bar", "baz"])
        self.assertCollection([], field="foo")
        self.assertCollection([obj._pk], field="bar")
        self.assertCollection([obj._pk], field="baz")
        self.assertCollection([], field="faz")

    def test_delete_list(self):
        obj = self.model()
        obj.field.rpush("foo", "bar", "baz", "faz")
        obj.field.delete()
        self.assertEqual(obj.field.proxy_get(), [])


class Menu(TestRedisModel):
    dishes = fields.ListField(unique=True)


class UniqueListFieldTest(BaseModelTest):

    model = Menu

    def test_unique_listfield_should_not_be_settable_twice_at_init(self):
        menu1 = self.model(dishes=['pasta', 'ravioli'])
        self.assertCollection([menu1._pk], dishes="pasta")
        with self.assertRaises(UniquenessError):
            self.model(dishes=['pardule', 'pasta'])
        self.assertCollection([menu1._pk], dishes="pasta")
        self.assertCollection([], dishes="pardule")

    def test_rpush_should_hit_the_uniqueness_check(self):
        menu1 = self.model()
        menu1.dishes.rpush('pasta', 'ravioli')
        self.assertCollection([menu1._pk], dishes="pasta")
        menu2 = self.model(dishes=['gniocchi', 'spaghetti'])
        with self.assertRaises(UniquenessError):
            menu2.dishes.rpush('pardule', 'pasta')
        self.assertCollection([menu1._pk], dishes="pasta")
        self.assertCollection([menu2._pk], dishes="gniocchi")
        self.assertCollection([menu2._pk], dishes="spaghetti")
        self.assertCollection([], dishes="pardule")

    def test_linsert_should_hit_the_uniqueness_check(self):
        menu1 = self.model(dishes=['pasta', 'ravioli'])
        self.assertCollection([menu1._pk], dishes="pasta")
        menu2 = self.model(dishes=['gniocchi', 'spaghetti'])
        with self.assertRaises(UniquenessError):
            menu2.dishes.rpush('before', 'spaghetti', 'pasta')
        self.assertCollection([menu1._pk], dishes="pasta")
        self.assertCollection([menu2._pk], dishes="gniocchi")

    def test_lset_should_hit_the_uniqueness_check(self):
        menu1 = self.model(dishes=['pasta', 'ravioli'])
        self.assertCollection([menu1._pk], dishes="pasta")
        menu2 = self.model(dishes=['gniocchi', 'spaghetti'])
        with self.assertRaises(UniquenessError):
            menu2.dishes.lset(0, 'pasta')
        self.assertCollection([menu1._pk], dishes="pasta")
        self.assertCollection([menu2._pk], dishes="gniocchi")
