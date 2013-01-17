# -*- coding:utf-8 -*-

import unittest

import threading
import time
from datetime import datetime

from limpyd import model
from limpyd import fields
from limpyd.exceptions import *
from base import LimpydBaseTest, TEST_CONNECTION_SETTINGS


class TestRedisModel(model.RedisModel):
    """
    Use it as a base for all RedisModel created for tests
    """
    database = LimpydBaseTest.database
    abstract = True
    namespace = "tests"  # not mandatory as namespace can be empty


class Bike(TestRedisModel):
    name = fields.StringField(indexable=True)
    wheels = fields.StringField(default=2)
    passengers = fields.StringField(default=1)


class MotorBike(Bike):
    power = fields.StringField()


class Boat(TestRedisModel):
    """
    Use also InstanceHashField.
    """

    name = fields.StringField(unique=True)
    power = fields.InstanceHashField(indexable=True, default="sail")
    launched = fields.StringField(indexable=True)
    length = fields.StringField()


class BaseModelTest(LimpydBaseTest):

    model = None

    def assertCollection(self, expected, **filters):
        self.assertEqual(
            set(self.model.collection(**filters)),
            set(expected)
        )


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

    def test_fields_should_be_ordered(self):
        self.assertEqual(Bike._fields, ['pk', 'name', 'wheels', 'passengers'])
        self.assertEqual(MotorBike._fields, ['pk', 'name', 'wheels', 'passengers', 'power'])
        motorbike = MotorBike()
        self.assertEqual(motorbike._fields, ['pk', 'name', 'wheels', 'passengers', 'power'])

    def test_lazy_connect_should_not_connect_to_redis(self):
        bike = Bike(name="rosalie", wheels=4)
        self.assertTrue(bike.connected)

        # get an object with an existing pk
        with self.assertNumCommands(0):
            bike2 = Bike.lazy_connect(bike._pk)
        self.assertFalse(bike2.connected)

        # test a field
        self.assertEqual(bike2.name.get(), 'rosalie')
        self.assertFalse(bike2.connected)

        # set a field
        bike2.name.set('velocipede')
        self.assertTrue(bike2.connected)

        # test if the value was correctly set
        with self.assertNumCommands(1):
            bike3 = Bike(bike._pk)
        self.assertEqual(bike3.name.get(), 'velocipede')

        # get an object with a non-existing pk
        with self.assertNumCommands(0):
            bike4 = Bike.lazy_connect(1000)
        # set a field: we check pk for the first update if skipped the existence test
        with self.assertRaises(DoesNotExist):
            bike4.name.set('monocycle')
        self.assertFalse(bike4.connected)

        # but we can get a field, no test is done here (simply return None if not exists)
        self.assertEqual(bike4.name.get(), None)

    def test_get_field_should_work_with_class_or_instance(self):
        self.assertEqual(Bike.get_field('name'), Bike._redis_attr_name)
        self.assertEqual(Bike.get_field('pk'), Bike._redis_attr_pk)
        bike = Bike()
        self.assertEqual(bike.get_field('name'), bike.name)
        self.assertEqual(bike.get_field('pk'), bike.pk)


class DatabaseTest(LimpydBaseTest):

    def test_database_should_be_mandatory(self):
        with self.assertRaises(ImplementationError):
            class WithoutDB(model.RedisModel):
                name = fields.StringField()

    def test_namespace_plus_model_should_be_unique(self):
        MainBike = Bike

        def sub_test():
            with self.assertRaises(ImplementationError):
                class Bike(TestRedisModel):
                    name = fields.StringField()

            class Bike(TestRedisModel):
                name = fields.StringField()
                namespace = 'sub-tests'
            self.assertNotEqual(MainBike._name, Bike._name)

        sub_test()

    def test_database_could_transfer_its_models_to_another(self):
        db1 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)
        db2 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)
        db3 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)

        class M(model.RedisModel):
            namespace = 'transfert-db-models'
            abstract = True

        class A(M):
            abstract = True

        class B(M):
            abstract = True

        class BA(B):
            database = db1

        class BAA(BA):
            pass

        class BAB(BA):
            pass

        class BB(B):
            database = db2

        class BBA(BB):
            pass

        class BBB(BB):
            abstract = True

        class BBBA(BBB):
            pass

        def assertModelsInDatabase(database, *models):
            """
            Test that the database contains all non-abstract models in the given
            list and that each model has the correct database.
            """
            self.assertEqual(database._models, dict((m._name, m) for m in models if not m.abstract))
            for m in models:
                self.assertEqual(m.database, database)

        def assertNoDatabase(*models):
            for m in models:
                self.assertFalse(hasattr(m, 'database'))

        # starting point
        assertNoDatabase(M, A, B)
        assertModelsInDatabase(db1, BA, BAA, BAB)
        assertModelsInDatabase(db2, BB, BBA, BBB, BBBA)
        assertModelsInDatabase(db3)

        # move B to db1
        B.use_database(db1)
        assertNoDatabase(M, A)  # B moved, alone, from here....
        assertModelsInDatabase(db1, B, BA, BAA, BAB)  # ...to here
        assertModelsInDatabase(db2, BB, BBA, BBB, BBBA)
        assertModelsInDatabase(db3)

        # move some models to db3
        B.use_database(db3)
        assertNoDatabase(M, A)
        assertModelsInDatabase(db1)  # B and submodels...
        assertModelsInDatabase(db3, B, BA, BAA, BAB)  # ...moved to db3
        assertModelsInDatabase(db2, BB, BBA, BBB, BBBA)  # models in db2 are still here

        # move back some to db1
        BA.use_database(db1)
        assertNoDatabase(M, A)
        assertModelsInDatabase(db1, BA, BAA, BAB)  # BA and submodels are here now...
        assertModelsInDatabase(db3, B)  # ...not here anymore
        assertModelsInDatabase(db2, BB, BBA, BBB, BBBA)

        # move some from db2 to db3
        BBB.use_database(db3)
        assertNoDatabase(M, A)
        assertModelsInDatabase(db1, BA, BAA, BAB)
        assertModelsInDatabase(db2, BB, BBA)  # BBB and submodel have moved...
        assertModelsInDatabase(db3, B, BBB, BBBA)  # ...here

        # move B alone in db2 (no direct submodel in db3)
        B.use_database(db2)
        assertNoDatabase(M, A)
        assertModelsInDatabase(db1, BA, BAA, BAB)
        assertModelsInDatabase(db2, B, BB, BBA)  # B is here now...
        assertModelsInDatabase(db3, BBB, BBBA)

        # move all from db3, to have a full chain
        BBB.use_database(db2)
        assertNoDatabase(M, A)
        assertModelsInDatabase(db1, BA, BAA, BAB)
        assertModelsInDatabase(db2, B, BB, BBA, BBB, BBBA)  # all from db3 is now here
        assertModelsInDatabase(db3)

        # and now move the B+BB chain in db3
        B.use_database(db3)
        assertNoDatabase(M, A)
        assertModelsInDatabase(db1, BA, BAA, BAB)
        assertModelsInDatabase(db2)  # nothing here anymore
        assertModelsInDatabase(db3, B, BB, BBA, BBB, BBBA)

        # move M, abstract without DB should move it's subclass A without DB
        M.use_database(db1)
        assertModelsInDatabase(db1, M, A, BA, BAA, BAB)  # hello M & A
        assertModelsInDatabase(db2)
        assertModelsInDatabase(db3, B, BB, BBA, BBB, BBBA)

    def test_use_database_shoud_be_threadsafe(self):
        """
        Check if when we use a new database, it's updated for the model on all
        threads
        """
        db1 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)
        db2 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)
        db3 = model.RedisDatabase(**TEST_CONNECTION_SETTINGS)

        class ThreadableModel(model.RedisModel):
            database = db1
            foo = fields.StringField()

        class Thread(threading.Thread):
            def __init__(self, test):
                self.test = test
                super(Thread, self).__init__()

            def run(self):
                # no reason to fail, but still test it
                self.test.assertEqual(ThreadableModel.database, db1)
                # wait a little to let the main thread set database to db2
                time.sleep(0.1)
                self.test.assertEqual(ThreadableModel.database, db2)
                # will be tested in main thread
                ThreadableModel.use_database(db3)

        thread = Thread(self)
        thread.start()

        # will be tested in child thread
        ThreadableModel.use_database(db2)

        # wait a little to let the child thread set database to db3
        time.sleep(0.2)
        self.assertEqual(ThreadableModel.database, db3)

    def test_database_should_accept_new_redis_connection_settings(self):
        some_settings = TEST_CONNECTION_SETTINGS.copy()
        some_settings['db'] = 14

        database = model.RedisDatabase(**some_settings)
        self.assertEqual(database.connection_settings['db'], 14)
        connection = database.connection

        database.reset(**TEST_CONNECTION_SETTINGS)
        self.assertEqual(database.connection_settings['db'], TEST_CONNECTION_SETTINGS['db'])
        self.assertNotEqual(connection, database.connection)


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
        boat2 = Boat.get(boat1._pk)
        self.assertEqual(boat1._pk, boat2._pk)
        self.assertEqual(boat1.name.get(), boat2.name.get())

    def test_should_filter_from_kwargs(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        boat2 = Boat.get(name="Pen Duick I")
        self.assertEqual(boat1._pk, boat2._pk)
        self.assertEqual(boat1.name.get(), boat2.name.get())
        boat3 = Boat.get(name="Pen Duick I", power="sail")
        self.assertEqual(boat1._pk, boat3._pk)
        self.assertEqual(boat1.name.get(), boat3.name.get())

    def test_should_accepte_a_simple_pk_as_kwargs(self):
        boat1 = Boat(name="Pen Duick I", length=15.1)
        with self.assertNumCommands(1):  # only a sismember
            boat2 = Boat.get(pk=boat1._pk)
        self.assertEqual(boat1._pk, boat2._pk)

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
        self.assertEqual(boat._pk, boat_again._pk)
        self.assertFalse(created)

    def test_should_connect_if_object_do_not_exists(self):
        boat = Boat(name="Pen Duick I")
        boat_again, created = Boat.get_or_connect(name="Pen Duick II")
        self.assertNotEqual(boat._pk, boat_again._pk)
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
        Boat(name="Pen Duick I", length=15.1, launched=1898)
        Boat(name="Pen Duick II", length=13.6, launched=1964)
        Boat(name="Pen Duick III", length=17.45, launched=1966)
        self.assertTrue(Boat.exists(pk=1))
        self.assertFalse(Boat.exists(pk=1000))
        self.assertTrue(Boat.exists(name="Pen Duick I"))
        self.assertTrue(Boat.exists(name="Pen Duick I", launched=1898))
        self.assertFalse(Boat.exists(name="Pen Duick II", launched=1898))
        self.assertFalse(Boat.exists(name="Pen Duick IV"))

    def test_should_raise_if_no_kwarg(self):
        with self.assertRaises(ValueError):
            Boat.exists()

    def test_doesnotexist_should_be_raised_when_object_not_found(self):
        with self.assertRaises(DoesNotExist):
            Boat(1000)
        with self.assertRaises(DoesNotExist):
            Boat.get(1000)
        with self.assertRaises(DoesNotExist):
            Boat.get(pk=1000)
        with self.assertRaises(DoesNotExist):
            Boat.get(name='France')


class MetaRedisProxyTest(LimpydBaseTest):

    def test_available_commands(self):
        """
        available_commands must exists on Fields and it must contain getters and modifiers.
        """
        def check_available_commands(cls):
            for command in cls.available_getters:
                self.assertTrue(command in cls.available_commands)
            for command in cls.available_modifiers:
                self.assertTrue(command in cls.available_commands)
        check_available_commands(fields.StringField)
        check_available_commands(fields.InstanceHashField)
        check_available_commands(fields.SortedSetField)
        check_available_commands(fields.SetField)
        check_available_commands(fields.ListField)
        check_available_commands(fields.PKField)
        check_available_commands(fields.AutoPKField)
        check_available_commands(fields.HashField)


class PostCommandTest(LimpydBaseTest):

    class MyModel(TestRedisModel):
        name = fields.InstanceHashField()
        last_modification_date = fields.InstanceHashField()

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


class DeleteTest(LimpydBaseTest):

    def test_stringfield_keys_are_deleted(self):

        class Train(TestRedisModel):
            namespace = "test_stringfield_keys_are_deleted"
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

    def test_instancehashfield_keys_are_deleted(self):

        class Train(TestRedisModel):
            namespace = "test_instancehashfield_keys_are_deleted"
            name = fields.InstanceHashField(unique=True)
            kind = fields.InstanceHashField(indexable=True)
            wagons = fields.InstanceHashField(default=10)

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

        class Train(TestRedisModel):
            namespace = "test_pkfield_cannot_be_deleted"
            name = fields.InstanceHashField(unique=True)

        train = Train(name="TGV")
        with self.assertRaises(ImplementationError):
            train.pk.delete()

    def test_model_delete(self):

        class Train(TestRedisModel):
            namespace = "test_model_delete"
            name = fields.InstanceHashField(unique=True)
            kind = fields.StringField(indexable=True)
            wagons = fields.InstanceHashField(default=10)

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
        class_config = Bike.database.connection_settings
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


class FieldExistenceTest(LimpydBaseTest):

    def test_unset_field_does_not_exist(self):
        boat = Boat(name="Pen Duick I")
        self.assertFalse(boat.length.exists())

    def test_field_with_default_value_exists(self):
        boat = Boat(name="Pen Duick I")
        self.assertTrue(boat.power.exists())

    def test_field_with_set_value_exists(self):
        boat = Boat(name="Pen Duick I")
        # test value given on init
        self.assertTrue(boat.name.exists())
        # test value manually set (StringField)
        boat.length.set(1)
        self.assertTrue(boat.length.exists())
        # test value manually set (InstanceHashField)
        boat.power.hset('engine')
        self.assertTrue(boat.power.exists())

    def test_deleted_field_does_not_exist(self):
        boat = Boat(name="Pen Duick I")
        # test InstanceHashField
        boat.power.delete()
        self.assertFalse(boat.power.exists())
        # test StringField
        boat.length.set(1)
        boat.length.delete()
        self.assertFalse(boat.length.exists())

    def test_field_of_deleted_object_does_not_exist(self):
        boat = Boat(name="Pen Duick I")
        boat.delete()
        # test InstanceHashField
        self.assertFalse(boat.power.exists())
        # test StringField
        self.assertFalse(boat.length.exists())

    def test_pk_field_exists(self):
        boat = Boat(name="Pen Duick I")
        self.assertTrue(boat.pk.exists())

    def test_deleted_pk_does_not_exist(self):
        boat = Boat(name="Pen Duick I")
        same_boat = Boat(boat._pk)
        boat.delete()
        self.assertFalse(boat.pk.exists())
        self.assertFalse(same_boat.pk.exists())


class ProxyTest(LimpydBaseTest):

    def test_proxy_get_should_call_real_getter(self):
        bike = Bike(name="rosalie", wheels=4)
        self.assertEqual(bike.name.proxy_get(), "rosalie")
        boat = Boat(name="Rainbow Warrior I", power="engine", length=40, launched=1955)
        self.assertEqual(boat.power.proxy_get(), "engine")

    def test_proxy_set_should_call_real_setter(self):
        bike = Bike(name="rosalia", wheels=4)
        bike.name.proxy_set('rosalie')
        self.assertEqual(bike.name.get(), "rosalie")
        boat = Boat(name="Rainbow Warrior I", power="human", length=40, launched=1955)
        boat.power.proxy_set('engine')
        self.assertEqual(boat.power.hget(), "engine")


if __name__ == '__main__':
    unittest.main()
