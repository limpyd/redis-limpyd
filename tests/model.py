# -*- coding:utf-8 -*-

import unittest

from limpyd import model
from limpyd import fields
from limpyd.exceptions import *
from base import LimpydBaseTest


class Bike(model.RedisModel):
    name = fields.StringField(indexable=True)
    wheels = fields.StringField(default=2)
    passengers = fields.StringField(default=1, cacheable=False)


class MotorBike(Bike):
    power = fields.StringField()


class Boat(model.RedisModel):
    """
    Use also HashableField.
    """
    cacheable = False

    name = fields.StringField(unique=True)
    power = fields.HashableField(indexable=True, default="sail")
    launched = fields.StringField(indexable=True)
    length = fields.StringField()


class InitTest(LimpydBaseTest):

    def test_instanciation_should_no_connect(self):
        bike = Bike()
        self.assertEqual(bike._pk, None)

    def test_setting_a_field_should_connect(self):
        bike = Bike()
        bike.name.set('rosalie')
        self.assertEqual(bike._pk, 1)

    def test_instances_must_not_share_fields(self):
        bike1 = Bike(name="rosalie")
        self.assertEqual(bike1._pk, 1)
        self.assertEqual(bike1.name.get(), "rosalie")
        bike2 = Bike(name="velocipede")
        self.assertEqual(bike2._pk, 2)
        self.assertEqual(bike2.name.get(), "velocipede")

    def test_field_instance_must_be_consistent(self):
        bike1 = Bike(name="rosalie")
        self.assertEqual(id(bike1), id(bike1.name._instance))
        bike2 = Bike(name="velocipede")
        self.assertEqual(id(bike2), id(bike2.name._instance))
        self.assertNotEqual(id(bike1.name._instance), id(bike2.name._instance))

    def test_one_arg_should_retrieve_from_db(self):
        bike = Bike()
        bike.name.set('rosalie')
        self.assertEqual(bike._pk, 1)
        bike_again = Bike(1)
        self.assertEqual(bike_again._pk, 1)

    def test_kwargs_should_be_setted_as_fields(self):
        bike = Bike(name="rosalie")
        self.assertEqual(bike.name.get(), "rosalie")

    def test_should_have_default_value_if_not_setted(self):
        bike = Bike(name="recumbent")
        self.assertEqual(bike.wheels.get(), '2')

    def test_default_value_should_not_override_setted_one(self):
        bike = Bike(name="rosalie", wheels=4)
        self.assertEqual(bike.wheels.get(), '4')

    def test_wrong_field_name_cannot_be_used(self):
        with self.assertRaises(ValueError):
            bike = Bike(power="human")


class IndexationTest(LimpydBaseTest):

    def test_stringfield_indexable(self):
        bike = Bike()
        bike.name.set("monocycle")
        self.assertFalse(Bike.exists(name="tricycle"))
        self.assertTrue(Bike.exists(name="monocycle"))
        bike.name.set("tricycle")
        self.assertFalse(Bike.exists(name="monocycle"))
        self.assertTrue(Bike.exists(name="tricycle"))


class CollectionTest(LimpydBaseTest):

    def test_new_instance_should_be_added_in_collection(self):
        self.assertEqual(Bike.collection(), set())
        bike = Bike()
        self.assertEqual(Bike.collection(), set())
        bike1 = Bike(name="trotinette")
        self.assertEqual(Bike.collection(), set(['1']))
        bike2 = Bike(name="tommasini")
        self.assertEqual(Bike.collection(), set(['1', '2']))

    def test_filter_from_kwargs(self):
        self.assertEqual(Boat.collection(), set())
        boat1 = Boat(name="Pen Duick I", length=15.1, launched=1898)
        boat2 = Boat(name="Pen Duick II", length=13.6, launched=1964)
        boat3 = Boat(name="Pen Duick III", length=17.45, launched=1966)
        boat4 = Boat(name="Rainbow Warrior I", power="engine", length=40, launched=1955)
        self.assertEqual(len(Boat.collection()), 4)
        self.assertEqual(len(Boat.collection(power="sail")), 3)
        self.assertEqual(len(Boat.collection(power="sail", launched=1966)), 1)

    def test_should_raise_if_filter_is_not_indexable_field(self):
        boat = Boat(name="Pen Duick I", length=15.1)
        with self.assertRaises(ValueError):
            Boat.collection(length=15.1)


class GetTest(LimpydBaseTest):

    def test_should_considere_one_arg_as_pk(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        boat2 = Boat.get(boat1.pk)
        self.assertEqual(boat1.pk, boat2.pk)
        self.assertEqual(boat1.name.get(), boat2.name.get())

    def test_should_filter_from_kwargs(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        boat2 = Boat.get(name="Pen Duick I")
        self.assertEqual(boat1.pk, boat2.pk)
        self.assertEqual(boat1.name.get(), boat2.name.get())
        boat3 = Boat.get(name="Pen Duick I", power="sail")
        self.assertEqual(boat1.pk, boat3.pk)
        self.assertEqual(boat1.name.get(), boat3.name.get())

    def test_should_raise_if_more_than_one_match(self):
        boat1 = Boat(name="Pen Duick I")
        boat2 = Boat(name="Pen Duick II")
        with self.assertRaises(ValueError):
            boat3 = Boat.get(power="sail")

    def test_should_raise_if_no_one_match(self):
        boat1 = Boat(name="Pen Duick I")
        with self.assertRaises(DoesNotExist):
            boat3 = Boat.get(name="Pen Duick II")

    def test_should_not_accept_more_than_one_arg(self):
        with self.assertRaises(ValueError):
            boat = Boat.get(1, 2)

    def test_should_not_accept_no_params(self):
        with self.assertRaises(ValueError):
            boat = Boat.get()


class GetOrConnectTest(LimpydBaseTest):

    def test_should_get_if_object_exists(self):
        boat = Boat(name="Pen Duick I")
        boat_again, created = Boat.get_or_connect(name="Pen Duick I")
        self.assertEqual(boat.pk, boat_again.pk)
        self.assertFalse(created)

    def test_should_connect_if_object_do_not_exists(self):
        boat = Boat(name="Pen Duick I")
        boat_again, created = Boat.get_or_connect(name="Pen Duick II")
        self.assertNotEqual(boat.pk, boat_again.pk)
        self.assertTrue(created)


class UniquenessTest(LimpydBaseTest):

    def test_cannot_set_unique_already_indexed_at_init(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        # First check data
        self.assertEqual(boat1.name.get(), "Pen Duick I")
        self.assertEqual(boat1.length.get(), "15.1")
        # Try to create a boat with the same name
        with self.assertRaises(UniquenessError):
            boat2 = Boat(name="Pen Duick I", length=15.1)
        # Check data after
        self.assertEqual(boat1.name.get(), "Pen Duick I")
        self.assertEqual(boat1.length.get(), "15.1")

    def test_cannot_set_unique_already_indexed_with_setter(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        # First check data
        self.assertEqual(boat1.name.get(), "Pen Duick I")
        self.assertEqual(boat1.length.get(), "15.1")
        boat2 = Boat(name="Pen Duick II", length=13.6)
        with self.assertRaises(UniquenessError):
            boat2.name.set("Pen Duick I")
        # Check data after
        self.assertEqual(boat1.name.get(), "Pen Duick I")
        self.assertEqual(boat1.length.get(), "15.1")


class ExistsTest(LimpydBaseTest):

    def test_generic_exists_test(self):
        boat1 = Boat(name="Pen Duick I", length=15.1, launched=1898)
        boat2 = Boat(name="Pen Duick II", length=13.6, launched=1964)
        boat3 = Boat(name="Pen Duick III", length=17.45, launched=1966)
        self.assertEqual(Boat.exists(name="Pen Duick I"), True)
        self.assertEqual(Boat.exists(name="Pen Duick I", launched=1898), True)
        self.assertEqual(Boat.exists(name="Pen Duick II", launched=1898), False)
        self.assertEqual(Boat.exists(name="Pen Duick IV"), False)

    def test_should_raise_if_no_kwarg(self):
        with self.assertRaises(ValueError):
            Boat.exists()


class CommandCacheTest(LimpydBaseTest):

    def test_should_not_hit_redis_when_cached(self):
        # Not sure the connection.info() is thread safe...
        bike = Bike(name="randonneuse")
        # First get
        name = bike.name.get()
        hits_before = self.connection.info()['keyspace_hits']
        # Get again
        name = bike.name.get()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(name, "randonneuse")
        self.assertEqual(hits_before, hits_after)
        # Flush all cache from instance
        bike.init_cache()
        name = bike.name.get()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(name, "randonneuse")
        self.assertNotEqual(hits_before, hits_after)

    def test_should_flush_if_modifiers_command_is_called(self):
        bike = Bike(name="draisienne")
        name = bike.name.get()
        self.assertEqual(name, "draisienne")
        bike.name.set('tandem')
        name = bike.name.get()
        self.assertEqual(name, "tandem")

    def test_should_not_hit_cache_when_flushed_for_field(self):
        bike = Bike(name="randonneuse", wheels=4)
        # First get
        name = bike.name.get()
        wheels = bike.wheels.get()
        hits_before = self.connection.info()['keyspace_hits']
        # Get again
        name = bike.name.get()
        wheels = bike.wheels.get()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(name, "randonneuse")
        self.assertEqual(wheels, "4")
        self.assertEqual(hits_before, hits_after)
        # Flush cache for field `name`
        bike.name.init_cache()
        name = bike.name.get()
        hits_after_flush = self.connection.info()['keyspace_hits']
        self.assertEqual(name, "randonneuse")
        self.assertNotEqual(hits_before, hits_after_flush)
        # Getting again the wheels must hit cache
        wheels = bike.wheels.get()
        hits_after_getting_wheels = self.connection.info()['keyspace_hits']
        self.assertEqual(wheels, "4")
        self.assertEqual(hits_after_flush, hits_after_getting_wheels)

    def test_not_cached_field_should_not_hit_cache(self):
        bike = Bike(name="tandem", wheels=2, passengers=2)
        # First get
        name = bike.name.get()
        passengers = bike.passengers.get()
        hits_before = self.connection.info()['keyspace_hits']
        # Get again
        name = bike.name.get()
        passengers = bike.passengers.get()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(name, "tandem")
        self.assertEqual(passengers, "2")
        hits_attended = hits_before + 1  # one field, `passengers`, should miss cache
        self.assertEqual(hits_after, hits_attended)

    def test_not_cached_model_should_not_hit_cache(self):
        boat = Boat(name="Pen Duick I", length=15.1, launched=1898)
        # First get
        name = boat.name.get()
        length = boat.length.get()
        launched = boat.launched.get()
        hits_before = self.connection.info()['keyspace_hits']
        # Get again
        name = boat.name.get()
        length = boat.length.get()
        launched = boat.launched.get()
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(name, "Pen Duick I")
        self.assertEqual(length, "15.1")
        self.assertEqual(launched, "1898")
        hits_attended = hits_before + 3  # the 3 fields should miss cache
        self.assertEqual(hits_after, hits_attended)


class MetaRedisProxyTest(LimpydBaseTest):

    def test_available_commands(self):
        """
        available_commands must exists on Fields and it must contain getters and modifiers.
        """
        def check_available_commands(cls):
            for command in cls.available_getters:
                self.assertTrue(command in cls.available_commands)
        check_available_commands(fields.StringField)
        check_available_commands(fields.HashableField)
        check_available_commands(fields.SortedSetField)
        check_available_commands(fields.SetField)
        check_available_commands(fields.ListField)


class InheritanceTest(LimpydBaseTest):

    def test_inheritance_fields(self):
        """
        Test that all fields are properly set on each model
        """
        bike = Bike()
        self.assertEqual(len(bike._fields), 3)
        self.assertEqual(set(bike._fields), set(['name', 'wheels', 'passengers']))
        motorbike = MotorBike()
        self.assertEqual(len(motorbike._fields), 4)
        self.assertEqual(set(motorbike._fields), set(['name', 'wheels', 'power', 'passengers']))
        boat = Boat()
        self.assertEqual(len(boat._fields), 4)
        self.assertEqual(set(boat._fields), set(['name', 'launched', 'power', 'length']))

    def test_inheritance_values(self):
        """
        Test that all values are correctly set on the good models
        """
        bike = Bike(name="rosalie", wheels=4)
        motorbike = MotorBike(name='davidson', wheels=2, power='not enough')
        self.assertEqual(bike.wheels.get(), '4')
        self.assertEqual(motorbike.wheels.get(), '2')
        self.assertEqual(motorbike.power.get(), 'not enough')

    def test_inheritance_collections(self):
        """
        Test that each model has its own collections
        """
        bike = Bike(name="rosalie", wheels=4)
        motorbike = MotorBike(name='davidson', wheels=2, power='not enough')
        self.assertEqual(len(Bike.collection(name="rosalie")), 1)
        self.assertEqual(len(Bike.collection(name="davidson")), 0)
        self.assertEqual(len(MotorBike.collection(name="rosalie")), 0)
        self.assertEqual(len(MotorBike.collection(name="davidson")), 1)


if __name__ == '__main__':
    unittest.main()
