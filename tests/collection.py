# -*- coding:utf-8 -*-

import unittest

from limpyd import model
from limpyd import fields
from limpyd.exceptions import *
from base import LimpydBaseTest, TEST_CONNECTION_SETTINGS
from model import Boat, Bike


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
        self.assertEqual(set(Bike.collection()), set(['1']))
        bike2 = Bike(name="tommasini")
        self.assertEqual(set(Bike.collection()), set(['1', '2']))

    def test_filter_from_kwargs(self):
        self.assertEqual(len(Boat.collection()), 4)
        self.assertEqual(len(Boat.collection(power="sail")), 3)
        self.assertEqual(len(Boat.collection(power="sail", launched=1966)), 1)

    def test_should_raise_if_filter_is_not_indexable_field(self):
        with self.assertRaises(ValueError):
            Boat.collection(length=15.1)


class SortTest(CollectionBaseTest):
    """
    Test the sort() method.
    """

    def test_temporary_key_is_deleted(self):
        """
        A temporary key is created for sorting, check that it is deleted.
        """
        keys_before = self.connection.info()['db%s' % TEST_CONNECTION_SETTINGS['db']]['keys']
        s = Boat.collection().sort()
        keys_after = self.connection.info()['db%s' % TEST_CONNECTION_SETTINGS['db']]['keys']
        self.assertEqual(keys_after, keys_before)

    def test_sort_without_argument_should_be_numeric(self):
        self.assertEqual(
            set(Boat.collection().sort()),
            set(['1', '2', '3', '4'])
        )

    def test_sort_should_be_scliceable(self):
        self.assertEqual(
            set(Boat.collection().sort()[1:3]),
            set(['2', '3'])
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


if __name__ == '__main__':
    unittest.main()
