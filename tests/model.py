# -*- coding:utf-8 -*-

import unittest

import threading
import time
from datetime import datetime
from redis.exceptions import RedisError

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
    passengers = fields.StringField(default=1, cacheable=False)


class MotorBike(Bike):
    power = fields.StringField()


class Boat(TestRedisModel):
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

    def test_fields_should_be_ordered(self):
        self.assertEqual(Bike._fields, ['pk', 'name', 'wheels', 'passengers'])
        self.assertEqual(MotorBike._fields, ['pk', 'name', 'wheels', 'passengers', 'power'])
        motorbike = MotorBike()
        self.assertEqual(motorbike._fields, ['pk', 'name', 'wheels', 'passengers', 'power'])


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

        database.connect(**TEST_CONNECTION_SETTINGS)
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
            for command in cls.available_full_modifiers:
                self.assertTrue(command in cls.available_commands)
                self.assertTrue(command in cls.available_modifiers)
            for command in cls.available_partial_modifiers:
                self.assertTrue(command in cls.available_commands)
                self.assertTrue(command in cls.available_modifiers)
        check_available_commands(fields.StringField)
        check_available_commands(fields.HashableField)
        check_available_commands(fields.SortedSetField)
        check_available_commands(fields.SetField)
        check_available_commands(fields.ListField)
        check_available_commands(fields.PKField)
        check_available_commands(fields.AutoPKField)


class PostCommandTest(LimpydBaseTest):

    class MyModel(TestRedisModel):
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

    class AutoPkModel(TestRedisModel):
        name = fields.StringField(indexable=True)

    class RedefinedAutoPkModel(AutoPkModel):
        id = fields.AutoPKField()

    class NotAutoPkModel(TestRedisModel):
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

    def test_cannot_set_pk_with_two_names(self):
        with self.assertRaises(ValueError):
            self.RedefinedNotAutoPkField(name="foo", pk=1, id=2)


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

    def test_hashablefield_keys_are_deleted(self):

        class Train(TestRedisModel):
            namespace = "test_hashablefield_keys_are_deleted"
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

        class Train(TestRedisModel):
            namespace = "test_pkfield_cannot_be_deleted"
            name = fields.HashableField(unique=True)

        train = Train(name="TGV")
        with self.assertRaises(ImplementationError):
            train.pk.delete()

    def test_model_delete(self):

        class Train(TestRedisModel):
            namespace = "test_model_delete"
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


class HMTest(LimpydBaseTest):
    """
    Test behavior of hmset and hmget
    """

    class HMTestModel(TestRedisModel):
        foo = fields.HashableField()
        bar = fields.HashableField(indexable=True)
        baz = fields.HashableField(unique=True)

    def test_hmset_should_set_all_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        self.assertEqual(obj.foo.hget(), 'FOO')
        self.assertEqual(obj.bar.hget(), 'BAR')
        self.assertEqual(obj.baz.hget(), 'BAZ')

    def test_hmget_should_get_all_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        data = obj.hmget('foo', 'bar', 'baz')
        self.assertEqual(data, ['FOO', 'BAR', 'BAZ'])

    def test_hmset_should_index_values(self):
        obj = self.HMTestModel()
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        self.assertEqual(set(self.HMTestModel.collection(bar='BAR')), set([obj._pk]))
        self.assertEqual(set(self.HMTestModel.collection(baz='BAZ')), set([obj._pk]))

    def test_hmset_should_clear_cache_for_fields(self):
        obj = self.HMTestModel()
        obj.foo.hget()  # set the cache
        obj.hmset(foo='FOO', bar='BAR', baz='BAZ')
        hits_before = self.connection.info()['keyspace_hits']
        obj.foo.hget()  # should miss the cache and hit redis
        hits_after = self.connection.info()['keyspace_hits']
        self.assertEqual(hits_before + 1, hits_after)

    def test_hmset_should_not_index_if_an_error_occurs(self):
        self.HMTestModel(baz="BAZ")
        test_obj = self.HMTestModel()
        with self.assertRaises(UniquenessError):
            # The order of parameters below is important. Yes all are passed via
            # the kwargs dict, but order is not random, it's consistent, and
            # here i have to be sure that "bar" is managed first in hmset, so i
            # do some tests to always have the wanted order.
            # So bar will be indexed, then baz will raise because we already
            # set the "BAZ" value for this field.
            test_obj.hmset(baz='BAZ', foo='FOO', bar='BAR')
        # We must not have an entry in the bar index with the BAR value because
        # the hmset must have raise an exception and revert index already set.
        self.assertEqual(set(self.HMTestModel.collection(bar='BAR')), set())

    def test_hmget_should_get_values_from_cache(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        # fill the cache
        obj.foo.hget()

        # get it from cache
        hits_before = self.connection.info()['keyspace_hits']
        obj.hmget('foo')
        hits_after = self.connection.info()['keyspace_hits']
        # hmget should not have hit redis
        self.assertEqual(hits_before, hits_after)

        # get one from cache, one from redis
        hits_before = self.connection.info()['keyspace_hits']
        obj.hmget('foo', 'bar')
        hits_after = self.connection.info()['keyspace_hits']
        # hmget should have hit redis to get bar
        self.assertEqual(hits_before + 1, hits_after)

    def test_hmget_should_cache_retrieved_values_for_hget(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR', baz='BAZ')
        obj.hmget('foo', 'bar')
        with self.assertNumCommands(0):
            foo = obj.foo.hget()
            self.assertEqual(foo, 'FOO')

    def test_hmget_result_is_not_cached_itself(self):
        obj = self.HMTestModel(foo='FOO', bar='BAR')
        obj.hmget()
        obj.foo.hset('FOO2')
        with self.assertNumCommands(1):
            data = obj.hmget()
            self.assertEqual(data, ['FOO2', 'BAR', None])


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
        # test value manually set (HashableField)
        boat.power.hset('engine')
        self.assertTrue(boat.power.exists())

    def test_deleted_field_does_not_exist(self):
        boat = Boat(name="Pen Duick I")
        # test HashableField
        boat.power.delete()
        self.assertFalse(boat.power.exists())
        # test StringField
        boat.length.set(1)
        boat.length.delete()
        self.assertFalse(boat.length.exists())

    def test_field_of_deleted_object_does_not_exist(self):
        boat = Boat(name="Pen Duick I")
        boat.delete()
        # test HashableField
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


class IndexableSortedSetFieldTest(LimpydBaseTest):

    class SortedSetModel(TestRedisModel):
        field = fields.SortedSetField(indexable=True)

    def test_indexable_sorted_sets_are_indexed(self):
        obj = self.SortedSetModel()

        # add one value
        obj.field.zadd(1.0, 'foo')
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())

        # add another value
        with self.assertNumCommands(5):
            # check that only 5 commands occured: zadd + index of value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zadd(2.0, 'bar')
        # check collections
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set([obj._pk]))

        # remove a value
        with self.assertNumCommands(5):
            # check that only 5 commands occured: zrem + deindex of value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zrem('foo')
        # check collections
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set([obj._pk]))

        # remove the object
        obj.delete()
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())

    def test_zincr_should_correctly_index_only_its_own_value(self):
        obj = self.SortedSetModel()

        # add a value, to check that its index is not updated
        obj.field.zadd(ignorable=1)

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for zincr, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zincrby('foo', 5.0)

        # check that the new value is indexed
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set([obj._pk]))

        # check that the previous value was not deindexed
        self.assertEqual(set(self.SortedSetModel.collection(field='ignorable')), set([obj._pk]))

    def test_zremrange_reindex_all_vaues(self):
        obj = self.SortedSetModel()

        obj.field.zadd(foo=1, bar=2, baz=3)

        # we remove two values
        with self.assertNumCommands(10):
            # check that we had 10 commands:
            # - 1 to get all existing values to deindex
            # - 3 to deindex all values
            # - 1 for the zremrange
            # - 1 to get all remaining values to index
            # - 1 to index the only remaining value
            # - 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.zremrangebyscore(1, 2)

        # check that all values are correctly indexed/deindexed
        self.assertEqual(set(self.SortedSetModel.collection(field='foo')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='bar')), set())
        self.assertEqual(set(self.SortedSetModel.collection(field='baz')), set([obj._pk]))


class IndexableSetFieldTest(LimpydBaseTest):

    class SetModel(TestRedisModel):
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

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for spop, one for deindexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            poped_value = obj.field.spop()

        values.remove(poped_value)
        self.assertEqual(obj.field.proxy_get(), set(values))
        self.assertEqual(set(self.SetModel.collection(field=values[0])), set([obj._pk]))
        self.assertEqual(set(self.SetModel.collection(field=poped_value)), set())


class IndexableListFieldTest(LimpydBaseTest):

    class ListModel(TestRedisModel):
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

        with self.assertNumCommands(5):
            # check that we had only 5 commands: one for lpop, one for deindexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            bar = obj.field.lpop()

        self.assertEqual(bar, 'bar')
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='bar')), set())

    def test_pushx_commands_should_correctly_index_only_its_values(self):
        obj = self.ListModel()

        # check that pushx on an empty list does nothing
        obj.field.lpushx('foo')
        self.assertEqual(obj.field.proxy_get(), [])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())

        # add a value to really test pushx
        obj.field.lpush('foo')
        # then test pushx
        with self.assertNumCommands(5):
            # check that we had only 5 comands, one for the rpushx, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.rpushx('bar')

        # test list and collection, to be sure
        self.assertEqual(obj.field.proxy_get(), ['foo', 'bar'])
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

    def test_lrem_command_should_correctly_deindex_only_its_value_when_possible(self):
        obj = self.ListModel()

        obj.field.lpush('foo', 'bar', 'foo',)

        #remove all foo
        with self.assertNumCommands(5):
            # check that we had only 5 comands, one for the lrem, one for indexing the value
            # + 3 for the lock (set at the biginning, check/unset at the end))
            obj.field.lrem(0, 'foo')

        # no more foo in the list
        self.assertEqual(obj.field.proxy_get(), ['bar'])
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

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
        self.assertEqual(set(self.ListModel.collection(field='foo')), set([obj._pk]))

    def test_lset_command_should_correctly_deindex_and_index_its_value(self):
        obj = self.ListModel()

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
        self.assertEqual(set(self.ListModel.collection(field='foo')), set())
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))

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
        self.assertEqual(set(self.ListModel.collection(field='bar')), set([obj._pk]))
        self.assertEqual(set(self.ListModel.collection(field='baz')), set())


if __name__ == '__main__':
    unittest.main()
