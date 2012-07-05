# -*- coding:utf-8 -*-

import unittest

from datetime import datetime
from redis.exceptions import RedisError

from limpyd import model
from limpyd import fields
from limpyd.exceptions import *
from base import LimpydBaseTest, TEST_CONNECTION_SETTINGS


class TestModelConnectionMixin(object):
    """
    Use it in first class for all RedisModel created for tests, or define
    the following settings in each
    """
    CONNECTION_SETTINGS = TEST_CONNECTION_SETTINGS


class Bike(TestModelConnectionMixin, model.RedisModel):
    name = fields.StringField(indexable=True)
    wheels = fields.StringField(default=2)
    passengers = fields.StringField(default=1, cacheable=False)


class MotorBike(Bike):
    power = fields.StringField()


class Boat(TestModelConnectionMixin, model.RedisModel):
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
        self.assertEqual(bike._pk, '1')

    def test_instances_must_not_share_fields(self):
        bike1 = Bike(name="rosalie")
        self.assertEqual(bike1._pk, '1')
        self.assertEqual(bike1.name.get(), "rosalie")
        bike2 = Bike(name="velocipede")
        self.assertEqual(bike2._pk, '2')
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
        self.assertEqual(bike._pk, '1')
        bike_again = Bike(1)
        self.assertEqual(bike_again._pk, '1')

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


class GetAttrTest(LimpydBaseTest):

    def test_get_redis_command(self):
        bike = Bike(name="monocycle")
        self.assertEqual(getattr(bike.name, 'get')(), "monocycle")
        with self.assertRaises(AttributeError):
            getattr(bike.name, 'hget')

    def test_get_normal_attr(self):
        bike = Bike(name="monocycle")
        self.assertEqual(getattr(bike, '_pk'), bike.pk.get())
        with self.assertRaises(AttributeError):
            getattr(bike, '_not_an_attr')


class IndexationTest(LimpydBaseTest):

    def test_stringfield_indexable(self):
        bike = Bike()
        bike.name.set("monocycle")
        self.assertFalse(Bike.exists(name="tricycle"))
        self.assertTrue(Bike.exists(name="monocycle"))
        bike.name.set("tricycle")
        self.assertFalse(Bike.exists(name="monocycle"))
        self.assertTrue(Bike.exists(name="tricycle"))

    def test_unicode_string_is_indexable(self):
        bike = Bike(name=u"vélo")
        self.assertFalse(Bike.exists(name="velo"))
        self.assertTrue(Bike.exists(name=u"vélo"))


class GetTest(LimpydBaseTest):

    def test_should_considere_one_arg_as_pk(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        boat2 = Boat.get(boat1.get_pk())
        self.assertEqual(boat1.get_pk(), boat2.get_pk())
        self.assertEqual(boat1.name.get(), boat2.name.get())

    def test_should_filter_from_kwargs(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        boat2 = Boat.get(name="Pen Duick I")
        self.assertEqual(boat1.get_pk(), boat2.get_pk())
        self.assertEqual(boat1.name.get(), boat2.name.get())
        boat3 = Boat.get(name="Pen Duick I", power="sail")
        self.assertEqual(boat1.get_pk(), boat3.get_pk())
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
        self.assertEqual(boat.get_pk(), boat_again.get_pk())
        self.assertFalse(created)

    def test_should_connect_if_object_do_not_exists(self):
        boat = Boat(name="Pen Duick I")
        boat_again, created = Boat.get_or_connect(name="Pen Duick II")
        self.assertNotEqual(boat.get_pk(), boat_again.get_pk())
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


class PostCommandTest(LimpydBaseTest):

    class MyModel(TestModelConnectionMixin, model.RedisModel):
        name = fields.HashableField()
        last_modification_date = fields.HashableField()

        def post_command(self, sender, name, result, args, kwargs):
            if isinstance(sender, fields.RedisField) and sender.name == "name":
                if name in sender.available_modifiers:
                    self.last_modification_date.hset(datetime.now())
                elif name == "hget":
                    result = "modifed_result"
            return result

    def test_instance_post_command_is_called(self):
        inst = self.MyModel()
        self.assertIsNone(inst.last_modification_date.hget())
        inst.name.hset("foo")
        # If post command has been called, last_modification_date must have changed
        self.assertIsNotNone(inst.last_modification_date.hget())
        last_modification_date = inst.last_modification_date.hget()
        # Change field again
        inst.name.hset("bar")
        self.assertNotEqual(last_modification_date, inst.last_modification_date.hget())

    def test_result_is_returned(self):
        inst = self.MyModel(name="foo")
        self.assertEqual("modifed_result", inst.name.hget())


class InheritanceTest(LimpydBaseTest):

    def test_inheritance_fields(self):
        """
        Test that all fields are properly set on each model
        """
        bike = Bike()
        self.assertEqual(len(bike._fields), 4)
        self.assertEqual(set(bike._fields), set(['pk', 'name', 'wheels', 'passengers']))
        motorbike = MotorBike()
        self.assertEqual(len(motorbike._fields), 5)
        self.assertEqual(set(motorbike._fields), set(['pk', 'name', 'wheels', 'passengers', 'power']))
        boat = Boat()
        self.assertEqual(len(boat._fields), 5)
        self.assertEqual(set(boat._fields), set(['pk', 'name', 'launched', 'power', 'length']))

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


class PKFieldTest(LimpydBaseTest):

    class AutoPkModel(TestModelConnectionMixin, model.RedisModel):
        name = fields.StringField(indexable=True)

    class RedefinedAutoPkModel(AutoPkModel):
        id = fields.AutoPKField()

    class NotAutoPkModel(TestModelConnectionMixin, model.RedisModel):
        pk = fields.PKField()
        name = fields.StringField(indexable=True)

    class ExtendedNotAutoPkField(NotAutoPkModel):
        pass

    class RedefinedNotAutoPkField(AutoPkModel):
        id = fields.PKField()

    def test_pk_value_for_default_pk_field(self):
        obj = self.AutoPkModel(name="foo")
        self.assertEqual(obj._pk, '1')
        self.assertEqual(obj.get_pk(), obj._pk)
        self.assertEqual(obj.pk.get(), obj._pk)
        same_obj = self.AutoPkModel.get(obj._pk)
        self.assertEqual(same_obj._pk, obj._pk)
        always_same_obj = self.AutoPkModel.get(pk=obj._pk)
        self.assertEqual(always_same_obj._pk, obj._pk)
        obj2 = self.AutoPkModel(name="bar")
        self.assertEqual(obj2._pk, '2')

    def test_pk_value_for_redefined_auto_pk_field(self):
        obj = self.RedefinedAutoPkModel(name="foo")
        self.assertEqual(obj._pk, '1')
        self.assertEqual(obj.get_pk(), obj._pk)
        self.assertEqual(obj.pk.get(), obj._pk)
        self.assertEqual(obj.id.get(), obj._pk)
        same_obj = self.RedefinedAutoPkModel.get(obj._pk)
        self.assertEqual(same_obj._pk, obj._pk)
        always_same_obj = self.RedefinedAutoPkModel.get(pk=obj._pk)
        self.assertEqual(always_same_obj._pk, obj._pk)
        obj2 = self.RedefinedAutoPkModel(name="bar")
        self.assertEqual(obj2._pk, '2')

    def test_pk_value_for_not_auto_increment_pk_field(self):
        obj = self.NotAutoPkModel(name="evil", pk=666)
        self.assertEqual(obj._pk, '666')
        self.assertEqual(obj.get_pk(), obj._pk)
        self.assertEqual(obj.pk.get(), obj._pk)
        # test with real string
        obj2 = self.NotAutoPkModel(name="foo", pk="bar")
        self.assertEqual(obj2._pk, "bar")
        self.assertEqual(obj2.pk.get(), obj2._pk)
        same_obj2 = self.NotAutoPkModel.get("bar")
        self.assertEqual(obj2._pk, same_obj2.pk.get())
        # test uniqueness
        with self.assertRaises(UniquenessError):
            self.NotAutoPkModel(name="baz", pk="666")

    def test_cannot_define_already_user_defined_pk_field(self):
        with self.assertRaises(ImplementationError):
            class InvalidAutoPkModel(self.RedefinedAutoPkModel):
                uid = fields.AutoPKField()

    def test_cannot_set_pk_for_auto_increment_pk_field(self):
        with self.assertRaises(ValueError):
            self.AutoPkModel(name="foo", pk=1)
        with self.assertRaises(ValueError):
            self.RedefinedAutoPkModel(name="bar", pk=2)

    def test_forced_to_set_pk_for_not_auto_increment_pk_field(self):
        with self.assertRaises(ValueError):
            self.NotAutoPkModel(name="foo")
        with self.assertRaises(ValueError):
            self.ExtendedNotAutoPkField(name="foo")

    def test_no_collision_between_pk(self):
        self.NotAutoPkModel(name="foo", pk=1000)
        # same model, same pk
        with self.assertRaises(UniquenessError):
            self.NotAutoPkModel(name="bar", pk=1000)
        # other model, same pk
        self.assertEqual(self.ExtendedNotAutoPkField(name="bar", pk=1000)._pk, '1000')

    def test_collections_filtered_by_pk(self):
        # default auto pk
        self.AutoPkModel(name="foo")
        self.AutoPkModel(name="foo")
        self.assertEqual(set(self.AutoPkModel.collection(name="foo")), set(['1', '2']))
        self.assertEqual(set(self.AutoPkModel.collection(pk=1)), set(['1', ]))
        self.assertEqual(set(self.AutoPkModel.collection(name="foo", pk=1)), set(['1', ]))
        self.assertEqual(set(self.AutoPkModel.collection(name="foo", pk=3)), set())
        self.assertEqual(set(self.AutoPkModel.collection(name="bar", pk=1)), set())
        # specific pk
        self.NotAutoPkModel(name="foo", pk="100")
        self.NotAutoPkModel(name="foo", pk="200")
        self.assertEqual(set(self.NotAutoPkModel.collection(name="foo")), set(['100', '200']))
        self.assertEqual(set(self.NotAutoPkModel.collection(pk=100)), set(['100', ]))
        self.assertEqual(set(self.NotAutoPkModel.collection(name="foo", pk=100)), set(['100', ]))
        self.assertEqual(set(self.NotAutoPkModel.collection(name="foo", pk=300)), set())
        self.assertEqual(set(self.NotAutoPkModel.collection(name="bar", pk=100)), set())

    def test_pk_cannot_be_updated(self):
        obj = self.AutoPkModel(name="foo")
        with self.assertRaises(ValueError):
            obj.pk.set(2)
        obj2 = self.RedefinedAutoPkModel(name="bar")
        with self.assertRaises(ValueError):
            obj2.pk.set(2)
        with self.assertRaises(ValueError):
            obj2.id.set(2)
        with self.assertRaises(ValueError):
            obj2.id.set(3)
        obj3 = self.NotAutoPkModel(name="evil", pk=666)
        with self.assertRaises(ValueError):
            obj3.pk.set(777)

    def test_can_access_pk_with_two_names(self):
        # create via pk, get via id or pk
        self.RedefinedNotAutoPkField(name="foo", pk=1)
        same_obj = self.RedefinedNotAutoPkField.get(pk=1)
        same_obj2 = self.RedefinedNotAutoPkField.get(id=1)
        self.assertEqual(same_obj.pk.get(), same_obj2.pk.get())
        self.assertEqual(same_obj.id.get(), same_obj2.id.get())
        # create via id, get via id or pk
        self.RedefinedNotAutoPkField(name="foo", id=2)
        same_obj = self.RedefinedNotAutoPkField.get(pk=2)
        same_obj2 = self.RedefinedNotAutoPkField.get(id=2)
        self.assertEqual(same_obj._pk, same_obj2._pk)
        self.assertEqual(same_obj.id.get(), same_obj2.id.get())
        # collection via pk or id
        self.assertEqual(set(self.RedefinedNotAutoPkField.collection(pk=1)), set(['1', ]))
        self.assertEqual(set(self.RedefinedNotAutoPkField.collection(id=2)), set(['2', ]))


class DeleteTest(LimpydBaseTest):

    def test_stringfield_keys_are_deleted(self):

        class Train(TestModelConnectionMixin, model.RedisModel):
            name = fields.StringField(unique=True)
            kind = fields.StringField(indexable=True)
            wagons = fields.StringField(default=10)

        # Check that db is empty
        self.assertEqual(len(self.connection.keys()), 0)
        # Create two models, to check also that the other is not
        # impacted by the delete of some field
        train1 = Train(name="Occitan", kind="Corail")
        train2 = Train(name="Teoz", kind="Corail")
        # Check that data is stored
        # Here we must have 11 keys:
        # - the pk collection
        # - the train model max id
        # - the 2 name fields
        # - the 2 name index
        # - the 2 kind fields
        # - the kind index for "Corail"
        # - the 2 wagons fields
        self.assertEqual(len(self.connection.keys()), 11)
        # If we delete the name field, only 9 key must remain
        # the train1.name field and the name:"Occitan" index are deleted
        train1.name.delete()
        self.assertEqual(len(self.connection.keys()), 9)
        self.assertEqual(train1.name.get(), None)
        self.assertFalse(Train.exists(name="Occitan"))
        self.assertEqual(train1.wagons.get(), '10')
        self.assertEqual(train2.name.get(), 'Teoz')
        self.assertEqual(len(self.connection.keys()), 9)
        # Now if we delete the train1.kind, only one key is deleted
        # The kind:"Corail" is still used by train2
        train1.kind.delete()
        self.assertEqual(len(self.connection.keys()), 8)
        self.assertEqual(len(Train.collection(kind="Corail")), 1)

    def test_hashablefield_keys_are_deleted(self):

        class Train(TestModelConnectionMixin, model.RedisModel):
            name = fields.HashableField(unique=True)
            kind = fields.HashableField(indexable=True)
            wagons = fields.HashableField(default=10)

        # Check that db is empty
        self.assertEqual(len(self.connection.keys()), 0)
        # Create two models, to check also that the other is not
        # impacted by the delete of some field
        train1 = Train(name="Occitan", kind="Corail")
        train2 = Train(name="Teoz", kind="Corail")
        # Check that data is stored
        # Here we must have 7 keys:
        # - the pk collection
        # - the pk max id
        # - 2 trains hash key
        # - the 2 names index (one by value)
        # - the kind index
        self.assertEqual(len(self.connection.keys()), 7)
        # The train1 hash must have three fields (name, kind and wagons)
        self.assertEqual(self.connection.hlen(train1.key), 3)
        # If we delete the train1 name, only 6 key must remain
        # (the name index for "Occitan" must be deleted)
        train1.name.delete()
        self.assertEqual(len(self.connection.keys()), 6)
        self.assertEqual(self.connection.hlen(train1.key), 2)
        self.assertEqual(train1.name.hget(), None)
        self.assertFalse(Train.exists(name="Occitan"))
        self.assertEqual(train1.wagons.hget(), '10')
        self.assertEqual(train2.name.hget(), 'Teoz')
        self.assertEqual(len(self.connection.keys()), 6)
        # Now if we delete the train1.kind, no key is deleted
        # Only the hash field must be deleted
        # The kind:"Corail" is still used by train2
        train1.kind.delete()
        self.assertEqual(len(self.connection.keys()), 6)
        self.assertEqual(self.connection.hlen(train1.key), 1)
        self.assertEqual(len(Train.collection(kind="Corail")), 1)

    def test_pkfield_cannot_be_deleted(self):

        class Train(TestModelConnectionMixin, model.RedisModel):
            name = fields.HashableField(unique=True)

        train = Train(name="TGV")
        with self.assertRaises(ImplementationError):
            train.pk.delete()

    def test_model_delete(self):

        class Train(TestModelConnectionMixin, model.RedisModel):
            name = fields.HashableField(unique=True)
            kind = fields.StringField(indexable=True)
            wagons = fields.HashableField(default=10)

        # Check that db is empty
        self.assertEqual(len(self.connection.keys()), 0)
        # Create two models, to check also that the other is not
        # impacted by the delete of some field
        train1 = Train(name="Occitan", kind="Corail")
        train2 = Train(name="Teoz", kind="Corail")
        # Check that data is stored
        # Here we must have 9 keys:
        # - the pk collection
        # - the pk max id
        # - 2 trains hash key
        # - the 2 names index (one by value)
        # - the two kind keys
        # - the kind:Corail index
        self.assertEqual(len(self.connection.keys()), 9)
        # If we delete the train1, only 6 key must remain
        train1.delete()
        self.assertEqual(len(self.connection.keys()), 6)
        with self.assertRaises(DoesNotExist):
            train1.name.hget()
        with self.assertRaises(DoesNotExist):
            train1.kind.get()
        self.assertFalse(Train.exists(name="Occitan"))
        self.assertTrue(Train.exists(name="Teoz"))
        self.assertEqual(train2.name.hget(), 'Teoz')
        self.assertEqual(len(self.connection.keys()), 6)
        self.assertEqual(len(Train.collection(kind="Corail")), 1)
        self.assertEqual(len(Train.collection()), 1)


class ConnectionTest(LimpydBaseTest):

    def test_connection_is_the_one_defined(self):
        defined_config = TEST_CONNECTION_SETTINGS
        current_config = self.connection.connection_pool.connection_kwargs
        bike = Bike(name="rosalie", wheels=4)
        obj_config = bike.connection.connection_pool.connection_kwargs
        class_config = Bike.CONNECTION_SETTINGS
        for arg in ('host', 'port', 'db'):
            self.assertEqual(defined_config[arg], current_config[arg])
            self.assertEqual(defined_config[arg], obj_config[arg])
            self.assertEqual(defined_config[arg], class_config[arg])

    def test_connection_should_be_shared(self):
        first_connected = self.connection.info()['connected_clients']
        bike = Bike(name="rosalie", wheels=4)
        self.assertEqual(first_connected, self.connection.info()['connected_clients'])
        Bike.collection(name="rosalie")
        self.assertEqual(first_connected, self.connection.info()['connected_clients'])
        bike.name.set("randonneuse")
        self.assertEqual(first_connected, self.connection.info()['connected_clients'])
        boat = Boat(name="Pen Duick I", length=15.1, launched=1898)
        self.assertEqual(bike.connection, boat.connection)


class ProxyTest(LimpydBaseTest):

    def proxy_get_should_call_real_getter(self):
        bike = Bike(name="rosalie", wheels=4)
        self.assertEqual(bike.name.proxy_get(), "rosalie")
        boat = Boat(name="Rainbow Warrior I", power="engine", length=40, launched=1955)
        self.assertEqual(boat.power.proxy_get(), "engine")

    def proxy_set_should_call_real_setter(self):
        bike = Bike(name="rosalia", wheels=4)
        bike.name.proxy_set('rosalie')
        self.assertEqual(bike.name.get(), "rosaie")
        boat = Boat(name="Rainbow Warrior I", power="human", length=40, launched=1955)
        boat.power.proxy_set('engine')
        self.assertEqual(boat.power.hget(), "engine")


class IndexableSortedSetFieldTest(LimpydBaseTest):

    class SortedSetModel(TestModelConnectionMixin, model.RedisModel):
        field = fields.SortedSetField(indexable=True)

    def test_indexable_sorted_sets_are_indexed(self):
        obj = self.SortedSetModel()

        # add one value
        obj.field.zadd(1.0, 'foo')
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())

        # add another value
        commands_before = self.count_commands()
        obj.field.zadd(2.0, 'bar')
        commands_after = self.count_commands()
        # check collections
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set([obj._pk]))
        # check that only two commands occured: zadd + index of value
        self.assertEqual(commands_after - commands_before, 2)

        # remove a value
        commands_before = self.count_commands()
        obj.field.zrem('foo')
        commands_after = self.count_commands()
        # check collections
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set([obj._pk]))
        # check that only two commands occured: zrem + deindex of value
        self.assertEqual(commands_after - commands_before, 2)

        # remove the object
        obj.delete()
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())

    def test_zincr_should_correctly_index_only_its_own_value(self):
        obj = self.SortedSetModel()

        # add a value, to check that its index is not updated
        obj.field.zadd(ignorable=1)

        commands_before = self.count_commands()
        obj.field.zincrby('foo', 5.0)
        commands_after = self.count_commands()

        # check that the new value is indexed
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))

        # check that the previous value was not deindexed
        self.assertEqual(set(self.SortedSetModel.collection(field='ignorable')), set([obj._pk]))

        # check that we had only two commands: one for zincr, one for indexing the value
        self.assertEqual(commands_after - commands_before, 2)

    def test_zremrange_reindex_all_vaues(self):
        obj = self.SortedSetModel()

        obj.field.zadd(foo=1, bar=2, baz=3)

        # we remove two values
        commands_before = self.count_commands()
        obj.field.zremrangebyscore(1, 2)
        commands_after = self.count_commands()

        # check that all values are correctly indexed/deindexed
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='baz')), set([obj._pk]))

        # check that we had 7 commands:
        # - 1 to get all existing values to deindex
        # - 3 to deindex all values
        # - 1 for the zremrange
        # - 1 to get all remaining values to index
        # - 1 to index the only remaining value
        self.assertEqual(commands_after - commands_before, 7)


class IndexableSetFieldTest(LimpydBaseTest):

    class SetModel(TestModelConnectionMixin, model.RedisModel):
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
        commands_before = self.count_commands()
        poped_value = obj.field.spop()
        commands_after = self.count_commands()

        values.remove(poped_value)
        self.assertEqual(obj.field.proxy_get(), set(values))
        self.assertEqual(set(self.SetModel.collection(field=values[0])), set([obj._pk]))
        self.assertEqual(set(self.SetModel.collection(field=poped_value)), set())

        # check that we had only two commands: one for spop, one for deindexing the value
        self.assertEqual(commands_after - commands_before, 2)


class IndexableListFieldTest(LimpydBaseTest):

    class ListModel(TestModelConnectionMixin, model.RedisModel):
        field = fields.ListField(indexable=True)

    def test_indexable_lists_are_indexed(self):
        obj = self.ListModel()

        # add one value
        obj.field.lpush('foo')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

        # add another value
        obj.field.lpush('bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        # remove a value
        obj.field.rpop()  # will remove foo
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        obj.delete()
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

        # test we can add many values at the same time
        obj = self.ListModel()
        obj.field.rpush('foo', 'bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

    def test_pop_commands_should_correctly_deindex_one_value(self):
        obj = self.ListModel()

        obj.field.lpush('foo', 'bar')
        commands_before = self.count_commands()
        bar = obj.field.lpop()
        commands_after = self.count_commands()

        self.assertEqual(bar, 'bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

        # check that we had only two commands: one for lpop, one for deindexing the value
        self.assertEqual(commands_after - commands_before, 2)

    def test_pushx_commands_should_correctly_index_only_its_values(self):
        obj = self.ListModel()

        # check that pushx on an empty list does nothing
        obj.field.lpushx('foo')
        self.assertEqual(obj.field.proxy_get(), [])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())

        # add a value to really test pushx
        obj.field.lpush('foo')
        # then test pushx
        commands_before = self.count_commands()
        obj.field.rpushx('bar')
        commands_after = self.count_commands()

        # test list and collection, to be sure
        self.assertEqual(obj.field.proxy_get(), ['foo', 'bar'])
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

        # check that we had only two comands, one for the rpushx, one for indexing the value
        self.assertEqual(commands_after - commands_before, 2)

    def test_lrem_command_should_correctly_deindex_only_its_value_when_possible(self):
        obj = self.ListModel()

        obj.field.lpush('foo', 'bar', 'foo',)

        #remove all foo
        commands_before = self.count_commands()
        obj.field.lrem(0, 'foo')
        commands_after = self.count_commands()
        # no more foo in the list
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))
        # check that we had only two comands, one for the lrem, one for indexing the value
        self.assertEqual(commands_after - commands_before, 2)

        # add more foos to test lrem with another count parameter
        obj.field.lpush('foo')
        obj.field.rpush('foo')

        # remove foo at the start
        commands_before = self.count_commands()
        obj.field.lrem(1, 'foo')
        commands_after = self.count_commands()
        # still a foo in the list
        self.assertEqual(obj.field.proxy_get(), ['bar', 'foo'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))

        # we did a lot of calls to reindex, just check this:
        # - 1 lrange to get all values before the lrem
        # - 3 srem to deindex the 3 values (even if two values are the same)
        # - 1 lrem call
        # - 1 lrange to get all values after the rem
        # - 2 sadd to index the two remaining values
        self.assertEqual(commands_after - commands_before, 8)

    def test_lset_command_should_correctly_deindex_and_index_its_value(self):
        obj = self.ListModel()

        obj.field.lpush('foo')

        # replace foo with bar
        commands_before = self.count_commands()
        obj.field.lset(0, 'bar')
        commands_after = self.count_commands()
        # check collections
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))
        # we should have 4 calls:
        # - 1 lindex to get the current value
        # - 1 to deindex this value
        # - 1 for the lset call
        # - 1 to index the new value
        self.assertEqual(commands_after - commands_before, 4)

        # replace an inexisting value will raise, without (de)indexing anything
        commands_before = self.count_commands()
        with self.assertRaises(RedisError):
            obj.field.lset(1, 'baz')
        commands_after = self.count_commands()
        # check collections are not modified
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='baz')), set())
        # we should have 2 calls:
        # - 1 lindex to get the current value, which is None (out f range) so
        #   nothing to deindex
        # - 1 for the lset call
        self.assertEqual(commands_after - commands_before, 2)


if __name__ == '__main__':
    unittest.main()
