# -*- coding:utf-8 -*-

import unittest

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
        self.assertEqual(len(Boat.collection()), 4)
        self.assertEqual(len(Boat.collection(power="sail")), 3)
        self.assertEqual(len(Boat.collection(power="sail", launched=1966)), 1)

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
        self.assertEqual(len(Boat.collection()), 4)
        # only sail powered boats, using an other manager
        self.assertEqual(len(Boat.collection(manager=SailBoats)), 3)

        class ActiveGroups(CollectionManager):
            def __init__(self, cls):
                super(ActiveGroups, self).__init__(cls)
                self._add_filters(active=1)

        class Group(TestRedisModel):
            namespace = 'collection'
            collection_manager = ActiveGroups
            name = fields.HashableField()
            active = fields.HashableField(indexable=True, default=1)

        Group(name='limpyd core devs')
        Group(name='limpyd fan boys', active=0)

        # all active groups, using our filtered manager, attached to the model
        self.assertEqual(len(Group.collection()), 1)
        # all groups by using the default manager
        self.assertEqual(len(Group.collection(manager=CollectionManager)), 2)


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

    def test_sort_by_hashablefield(self):

        class Event(TestRedisModel):
            year = fields.HashableField()

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
            name = fields.HashableField()

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
            self.assertIn(instance.get_pk(), Boat.collection())

    def test_sort_should_return_instances(self):

        for instance in Boat.collection().instances().sort():
            self.assertTrue(isinstance(instance, Boat))

    def test_instances_can_be_filtered_sliced_and_sorted(self):
        """
        Try to chain all the collection possibilities.
        """
        class Band(TestRedisModel):
            name = fields.HashableField(unique=True)
            started_in = fields.HashableField()
            genre = fields.HashableField(indexable=True)

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
        boats = Boat.collection(pk=1).instances()
        self.assertEqual(len(boats), 1)
        self.assertTrue(isinstance(boats[0], Boat))

        boats = Boat.collection(pk=10).instances()
        self.assertEqual(len(boats), 0)

    def test_instances_should_work_if_filtering_on_pk_and_other_fields(self):
        boats = Boat.collection(pk=1, name="Pen Duick I").instances()
        self.assertEqual(len(boats), 1)
        self.assertTrue(isinstance(boats[0], Boat))

        boats = Boat.collection(pk=10, name="Pen Duick I").instances()
        self.assertEqual(len(boats), 0)

    def test_call_to_primary_keys_should_cancel_instances(self):
        boats = set(Boat.collection().instances().primary_keys())
        self.assertEqual(boats, set(['1', '2', '3', '4']))


class ValuesTest(CollectionBaseTest):
    def test_values_should_return_a_list_of_dicts(self):
        boats = list(Boat.collection().values('pk', 'name', 'launched'))
        self.assertEqual(len(boats), 4)
        for boat in boats:
            self.assertTrue(isinstance(boat, dict))
            self.assertEqual(set(boat.keys()), set(['pk', 'name', 'launched']))
            test_boat = Boat(boat['pk'])
            self.assertEqual(test_boat.name.get(), boat['name'])
            self.assertEqual(test_boat.launched.get(), boat['launched'])

    def test_values_without_argument_returns_all_fields(self):
        boats = list(Boat.collection().values())
        self.assertEqual(len(boats), 4)
        self.assertTrue(isinstance(boats[0], dict))
        self.assertEqual(set(boats[0].keys()), set(['pk', 'name', 'power', 'launched', 'length']))

    def test_values_should_only_accept_simple_fields(self):
        with self.assertRaises(ValueError):
            # test a field that does not exist
            Boat.collection().values('foo')

        class BoatWithSet(Boat):
            passengers = fields.SetField()

        with self.assertRaises(ValueError):
            # test a field that is not a *simple* field
            Boat.collection().values('passengers')

    def test_values_should_accept_pk(self):
        #... but pk only has no advantage over simple collection result
        boats = list(Boat.collection(pk=self.boat1.get_pk()).values('pk'))
        self.assertEqual(boats[0]['pk'], self.boat1.get_pk())

        class BoatWithNewPk(Boat):
            id = fields.AutoPKField()
        boat = BoatWithNewPk()

        boats = list(BoatWithNewPk.collection(pk=boat.get_pk()).values('pk'))
        self.assertFalse('id' in boats[0])
        self.assertEqual(boats[0]['pk'], boat.get_pk())

        boats = list(BoatWithNewPk.collection(pk=boat.get_pk()).values('id'))
        self.assertFalse('pk' in boats[0])
        self.assertEqual(boats[0]['id'], boat.get_pk())

    def test_call_to_primary_keys_should_cancel_values(self):
        boats = set(Boat.collection().values('pk', 'name', 'launched').primary_keys())
        self.assertEqual(boats, set(['1', '2', '3', '4']))


class ValuesListTest(CollectionBaseTest):
    def test_values_list_should_return_a_list_of_tuples(self):
        boats = list(Boat.collection().values_list('pk', 'name', 'launched'))
        self.assertEqual(len(boats), 4)
        for boat in boats:
            self.assertTrue(isinstance(boat, tuple))
            self.assertTrue(len(boat), 3)
            test_boat = Boat(boat[0])
            self.assertEqual(test_boat.name.get(), boat[1])
            self.assertEqual(test_boat.launched.get(), boat[2])

    def test_values_list_without_argument_returns_all_fields(self):
        boats = list(Boat.collection().values_list())
        self.assertEqual(len(boats), 4)
        self.assertTrue(isinstance(boats[0], tuple))
        self.assertEqual(len(boats[0]), 5)
        test_boat = Boat(boats[0][0])
        self.assertEqual(test_boat.name.get(), boats[0][1])
        self.assertEqual(test_boat.power.hget(), boats[0][2])
        self.assertEqual(test_boat.launched.get(), boats[0][3])
        self.assertEqual(test_boat.length.get(), boats[0][4])

    def test_values_list_should_only_accept_simple_fields(self):
        with self.assertRaises(ValueError):
            # test a field that does not exist
            Boat.collection().values_list('foo')

        class BoatWithSet(Boat):
            namespace = Boat.namespace + 'values_list'
            passengers = fields.SetField()

        with self.assertRaises(ValueError):
            # test a field that is not a *simple* field
            Boat.collection().values_list('passengers')

    def test_values_list_should_accept_pk(self):
        #... but pk only has no advantage over simple collection result
        boats = list(Boat.collection(pk=self.boat1.get_pk()).values_list('pk'))
        self.assertEqual(boats[0][0], self.boat1.get_pk())

        class BoatWithNewPk(Boat):
            namespace = Boat.namespace + 'values_list'
            id = fields.AutoPKField()
        boat = BoatWithNewPk()

        boats = list(BoatWithNewPk.collection(pk=boat.get_pk()).values_list('pk'))
        self.assertEqual(boats[0][0], boat.get_pk())

        boats = list(BoatWithNewPk.collection(pk=boat.get_pk()).values_list('id'))
        self.assertEqual(boats[0][0], boat.get_pk())

    def test_flat_argument_should_return_flat_list(self):
        names = list(Boat.collection().values_list('name', flat=True))
        self.assertTrue(isinstance(names, list))
        self.assertEqual(names, [self.boat1.name.get(), self.boat2.name.get(),
                                 self.boat3.name.get(), self.boat4.name.get(), ])

    def test_flat_argument_should_be_refused_if_many_fields(self):
        with self.assertRaises(ValueError):
            Boat.collection().values_list('name', 'length', flat=True)

        with self.assertRaises(ValueError):
            Boat.collection().values_list(flat=True)

    def test_call_to_primary_keys_should_cancel_values_list(self):
        boats = set(Boat.collection().values_list('pk', 'name', 'launched').primary_keys())
        self.assertEqual(boats, set(['1', '2', '3', '4']))
        boats = set(Boat.collection().values_list('name', flat=True).primary_keys())
        self.assertEqual(boats, set(['1', '2', '3', '4']))

if __name__ == '__main__':
    unittest.main()
