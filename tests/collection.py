# -*- coding:utf-8 -*-

import unittest

from redis.exceptions import ResponseError
from limpyd import fields
from limpyd.collection import CollectionManager
from limpyd.exceptions import *
from base import LimpydBaseTest, TEST_CONNECTION_SETTINGS
from model import Boat, Bike, TestRedisModel


class CollectionBaseTest(LimpydBaseTest):

    def setUp(self):
        super(CollectionBaseTest, self).setUp()
        self.assertEqual(set(Boat.collection()), set())
        self.boat1 = Boat(name="Pen Duick I", length=15.1, launched=1898)
        self.boat2 = Boat(name="Pen Duick II", length=13.6, launched=1964)
        self.boat3 = Boat(name="Pen Duick III", length=17.45, launched=1966)
        self.boat4 = Boat(name="Rainbow Warrior I", power="engine", length=40, launched=1955)


class CollectionTest(CollectionBaseTest):
    """
    Test the collection filtering method.
    """

    def test_new_instance_should_be_added_in_collection(self):
        self.assertEqual(set(Bike.collection()), set())
        bike = Bike()
        self.assertEqual(set(Bike.collection()), set())
        bike1 = Bike(name="trotinette")
        self.assertEqual(set(Bike.collection()), set([bike1._pk]))
        bike2 = Bike(name="tommasini")
        self.assertEqual(set(Bike.collection()), set([bike1._pk, bike2._pk]))

    def test_filter_from_kwargs(self):
        self.assertEqual(len(list(Boat.collection())), 4)
        self.assertEqual(len(list(Boat.collection(power="sail"))), 3)
        self.assertEqual(len(list(Boat.collection(power="sail", launched=1966))), 1)

    def test_should_raise_if_filter_is_not_indexable_field(self):
        with self.assertRaises(ValueError):
            Boat.collection(length=15.1)

    def test_collection_should_be_lazy(self):
        # Simple collection
        hits_before = self.connection.info()['keyspace_hits']
        collection = Boat.collection()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(hits_before, hits_after)
        # Instances
        hits_before = self.connection.info()['keyspace_hits']
        collection = Boat.instances()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(hits_before, hits_after)
        # Filtered
        hits_before = self.connection.info()['keyspace_hits']
        collection = Boat.collection(power="sail")
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(hits_before, hits_after)
        # Slice it, it will be evaluated
        hits_before = self.connection.info()['keyspace_hits']
        collection = Boat.collection()[:2]
        hits_after = self.connection.info()['keyspace_hits']
        self.assertNotEqual(hits_before, hits_after)

    def test_collection_should_work_with_only_a_pk(self):
        hits_before = self.connection.info()['keyspace_hits']
        collection = list(Boat.collection(pk=1))
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(collection, ['1'])
        self.assertEqual(hits_before + 1, hits_after)  # only a sismembers

        hits_before = self.connection.info()['keyspace_hits']
        collection = list(Boat.collection(pk=5))
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(collection, [])
        self.assertEqual(hits_before + 1, hits_after)  # only a sismembers

    def test_collection_should_work_with_pk_and_other_fields(self):
        collection = list(Boat.collection(pk=1, name="Pen Duick I"))
        self.assertEqual(collection, ['1'])
        collection = list(Boat.collection(pk=1, name="Pen Duick II"))
        self.assertEqual(collection, [])
        collection = list(Boat.collection(pk=5, name="Pen Duick I"))
        self.assertEqual(collection, [])

    def test_collection_should_accept_pk_field_name_and_pk(self):
        class Person(TestRedisModel):
            namespace = 'collection'
            id = fields.AutoPKField()
            name = fields.StringField(indexable=True)

        Person(name='twidi')

        collection = list(Person.collection(id=1))
        self.assertEqual(collection, ['1'])

        collection = list(Person.collection(id=1, pk=1))
        self.assertEqual(collection, ['1'])

        collection = list(Person.collection(id=1, pk=2))
        self.assertEqual(collection, [])

    def test_connection_class_could_be_changed(self):
        class SailBoats(CollectionManager):
            def __init__(self, cls):
                super(SailBoats, self).__init__(cls)
                self._add_filters(power='sail')

        # all boats, using the default manager, attached to the model
        self.assertEqual(len(list(Boat.collection())), 4)
        # only sail powered boats, using an other manager
        self.assertEqual(len(list(Boat.collection(manager=SailBoats))), 3)

        class ActiveGroups(CollectionManager):
            def __init__(self, cls):
                super(ActiveGroups, self).__init__(cls)
                self._add_filters(active=1)

        class Group(TestRedisModel):
            namespace = 'collection'
            collection_manager = ActiveGroups
            name = fields.InstanceHashField()
            active = fields.InstanceHashField(indexable=True, default=1)

        Group(name='limpyd core devs')
        Group(name='limpyd fan boys', active=0)

        # all active groups, using our filtered manager, attached to the model
        self.assertEqual(len(list(Group.collection())), 1)
        # all groups by using the default manager
        self.assertEqual(len(list(Group.collection(manager=CollectionManager))), 2)


class SliceTest(CollectionBaseTest):
    """
    Test slicing of a collection
    """
    def test_get_one_item(self):
        collection = Boat.collection()
        self.assertEqual(collection[0], '1')

    def test_get_a_parts_of_the_collection(self):
        collection = Boat.collection()
        self.assertEqual(collection[1:3], ['2', '3'])

    def test_get_the_end_of_the_collection(self):
        collection = Boat.collection()
        self.assertEqual(collection[1:], ['2', '3', '4'])

    def test_using_netagive_index_should_work(self):
        collection = Boat.collection().sort()
        self.assertEqual(collection[-1], '4')
        self.assertEqual(collection[-2:4], ['3', '4'])

    def test_inexisting_slice_should_return_empty_collection(self):
        collection = Boat.collection()
        self.assertEqual(collection[5:10], [])

    def test_slicing_is_reset_on_next_call(self):
        # test whole content
        collection = Boat.collection()
        self.assertEqual(set(collection[1:]), set(['2', '3', '4']))
        self.assertEqual(set(collection), set(['1', '2',  '3', '4']))

        # test __iter__
        collection = Boat.collection()
        self.assertEqual(set(collection[1:]), set(['2', '3', '4']))
        all_pks = set([pk for pk in collection])
        self.assertEqual(all_pks, set(['1', '2',  '3', '4']))


class SortTest(CollectionBaseTest):
    """
    Test the sort() method.
    """

    def test_temporary_key_is_deleted(self):
        """
        A temporary key is created for sorting, check that it is deleted.
        """
        keys_before = self.connection.info()['db%s' % TEST_CONNECTION_SETTINGS['db']]['keys']
        s = list(Boat.collection().sort())
        keys_after = self.connection.info()['db%s' % TEST_CONNECTION_SETTINGS['db']]['keys']
        self.assertEqual(keys_after, keys_before)

    def test_sort_without_argument_should_be_numeric(self):
        self.assertEqual(
            list(Boat.collection().sort()),
            ['1', '2', '3', '4']
        )

    def test_sort_should_be_scliceable(self):
        self.assertEqual(
            list(Boat.collection().sort()[1:3]),
            ['2', '3']
        )

    def test_sort_and_getitem(self):
        self.assertEqual(Boat.collection().sort()[0], '1')
        self.assertEqual(Boat.collection().sort()[1], '2')
        self.assertEqual(Boat.collection().sort()[2], '3')
        self.assertEqual(Boat.collection().sort()[3], '4')

    def test_sort_by_stringfield(self):
        self.assertEqual(
            list(Boat.collection().sort(by="length")),
            ['2', '1', '3', '4']
        )

    def test_sort_by_stringfield_desc(self):
        self.assertEqual(
            list(Boat.collection().sort(by="-length")),
            ['4', '3', '1', '2']
        )

    def test_sort_by_instancehashfield(self):

        class Event(TestRedisModel):
            year = fields.InstanceHashField()

        # Create some instances
        Event(year=2000)
        Event(year=1900)
        Event(year=1820)
        Event(year=1999)

        self.assertEqual(
            list(Event.collection().sort(by="year")),
            ['3', '2', '4', '1']
        )

        # Sort it desc
        self.assertEqual(
            list(Event.collection().sort(by="-year")),
            ['1', '4', '2', '3']
        )

    def test_sort_by_alpha(self):

        class Singer(TestRedisModel):
            name = fields.InstanceHashField()

        # Create some instances
        Singer(name="Jacques Higelin")
        Singer(name="Jacques Brel")
        Singer(name="Alain Bashung")
        Singer(name=u"Gérard Blanchard")

        self.assertEqual(
            list(Singer.collection().sort(by="name", alpha=True)),
            ['3', '4', '2', '1']
        )

        # Sort it desc
        self.assertEqual(
            list(Singer.collection().sort(by="-name", alpha=True)),
            ['1', '2', '4', '3']
        )

    def test_sort_should_work_with_a_single_pk_filter(self):
        boats = list(Boat.collection(pk=1).sort())
        self.assertEqual(len(boats), 1)
        self.assertEqual(boats[0], '1')

    def test_sort_should_work_with_pk_and_other_fields(self):
        boats = list(Boat.collection(pk=1, name="Pen Duick I").sort())
        self.assertEqual(len(boats), 1)
        self.assertEqual(boats[0], '1')


class InstancesTest(CollectionBaseTest):
    """
    Test the instances() method.
    """

    def test_instances_should_return_instances(self):

        for instance in Boat.collection().instances():
            self.assertTrue(isinstance(instance, Boat))
            self.assertIn(instance._pk, Boat.collection())

    def test_sort_should_return_instances(self):

        for instance in Boat.collection().instances().sort():
            self.assertTrue(isinstance(instance, Boat))

    def test_instances_can_be_filtered_sliced_and_sorted(self):
        """
        Try to chain all the collection possibilities.
        """
        class Band(TestRedisModel):
            name = fields.InstanceHashField(unique=True)
            started_in = fields.InstanceHashField()
            genre = fields.InstanceHashField(indexable=True)

        madrugada = Band(name="Madrugada", started_in="1992", genre="Alternative")
        radiohead = Band(name="Radiohead", started_in="1985", genre="Alternative")
        the_veils = Band(name="The Veils", started_in="2001", genre="Alternative")
        archive = Band(name="Archive", started_in="1994", genre="Progressive Rock")

        self.assertEqual(
            [band._pk for band in Band.collection(genre="Alternative").instances().sort(by="-started_in")[:2]],
            [the_veils._pk, madrugada._pk]
        )

        # Should work also with instances shortcut
        self.assertEqual(
            [band._pk for band in Band.instances(genre="Alternative").sort(by="started_in")[:2]],
            [radiohead._pk, madrugada._pk]
        )

        # Getitem should work also
        self.assertEqual(
            Band.instances(genre="Alternative").sort(by="started_in")[0]._pk,
            radiohead._pk
        )

    def test_skip_exist_test_should_not_test_pk_existence(self):
        with self.assertNumCommands(5):
            # 1 command for the collection, one to test each PKs (4 objects)
            list(Boat.collection().instances())
        with self.assertNumCommands(1):
            # 1 command for the collection, none to test PKs
            list(Boat.collection().instances(skip_exist_test=True))

    def test_instances_should_work_if_filtering_on_only_a_pk(self):
        boats = list(Boat.collection(pk=1).instances())
        self.assertEqual(len(boats), 1)
        self.assertTrue(isinstance(boats[0], Boat))

        boats = list(Boat.collection(pk=10).instances())
        self.assertEqual(len(boats), 0)

    def test_instances_should_work_if_filtering_on_pk_and_other_fields(self):
        boats = list(Boat.collection(pk=1, name="Pen Duick I").instances())
        self.assertEqual(len(boats), 1)
        self.assertTrue(isinstance(boats[0], Boat))

        boats = list(Boat.collection(pk=10, name="Pen Duick I").instances())
        self.assertEqual(len(boats), 0)

    def test_call_to_primary_keys_should_cancel_instances(self):
        boats = set(Boat.collection().instances().primary_keys())
        self.assertEqual(boats, set(['1', '2', '3', '4']))


class LenTest(CollectionBaseTest):

    def test_len_should_not_call_sort(self):
        collection = Boat.collection(power="sail")

        # sorting will fail because alpha is not set to True
        collection.sort(by='name')

        # len won't fail on sort, not called
        self.assertEqual(len(collection), 3)

        # real call => the sort will fail
        with self.assertRaises(ResponseError):
            self.assertEqual(len(list(collection)), 3)

    def test_len_call_could_be_followed_by_a_iter(self):
        collection = Boat.collection(power="sail")
        self.assertEqual(len(collection), 3)
        self.assertEqual(set(collection), set(['1', '2', '3']))

    def test_len_should_work_with_slices(self):
        collection = Boat.collection(power="sail")[1:3]
        self.assertEqual(len(collection), 2)

    def test_len_should_work_with_pk(self):
        collection = Boat.collection(pk=1)
        self.assertEqual(len(collection), 1)
        collection = Boat.collection(power="sail", pk=2)
        self.assertEqual(len(collection), 1)
        collection = Boat.collection(pk=10)
        self.assertEqual(len(collection), 0)

    def test_len_should_work_with_instances(self):
        collection = Boat.collection(power="sail").sort(by='name').instances()
        self.assertEqual(len(collection), 3)


if __name__ == '__main__':
    unittest.main()
