# -*- coding:utf-8 -*-

import unittest

from limpyd import fields
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

if __name__ == '__main__':
    unittest.main()
