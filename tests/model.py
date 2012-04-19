import unittest

from limpyd import model
from base import LimpydBaseTest

class Bike(model.RedisModel):
    name = model.StringField(indexable=True)
    wheels = model.StringField(default=2)

class InitTest(LimpydBaseTest):

    def test_instanciation_should_no_connect(self):
        bike = Bike()
        self.assertEqual(bike._pk, None)

    def test_setting_a_field_should_connect(self):
        bike = Bike()
        bike.name.set('rosalie')
        self.assertEqual(bike._pk, 1)

    def test_one_arg_should_retrieve_from_db(self):
        bike = Bike()
        bike.name.set('rosalie')
        self.assertEqual(bike._pk, 1)
        bike_again = Bike(1)
        self.assertEqual(bike_again._pk, 1)

    def test_kwargs_should_be_setted_as_fields(self):
        bike = Bike(name="rosalie")
        self.assertEqual(bike.name.get(), "rosalie")


class IndexationTest(LimpydBaseTest):

    def test_stringfield_indexable(self):
        bike = Bike()
        bike.name.set("monocycle")
        self.assertFalse(Bike.exists(name="tricycle"))
        self.assertTrue(Bike.exists(name="monocycle"))
        bike.name.set("tricycle")
        self.assertFalse(Bike.exists(name="monocycle"))
        self.assertTrue(Bike.exists(name="tricycle"))


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

    def test_should_flush_if_modifiers_command_is_called(self):
        bike = Bike(name="draisienne")
        name = bike.name.get()
        self.assertEqual(name, "draisienne")
        bike.name.set('tandem')
        name = bike.name.get()
        self.assertEqual(name, "tandem")


class MetaRedisProxyTest(LimpydBaseTest):

    def test_available_commands(self):
        """
        available_commands must exists on Fields and it must contain getters and modifiers.
        """
        def check_available_commands(cls):
            for command in cls.available_getters:
                self.assertTrue(command in cls.available_commands)
        check_available_commands(model.StringField)
        check_available_commands(model.HashableField)
        check_available_commands(model.SortedSetField)


if __name__ == '__main__':
    unittest.main()
