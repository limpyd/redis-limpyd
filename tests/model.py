import unittest

from limpyd import model
from base import LimpydBaseTest

class Bike(model.RedisModel):
    name = model.StringField(indexable=True)
    wheels = model.StringField(default=2)

class CreationTest(LimpydBaseTest):

    def test_dummy(self):
        bike = Bike()
        bike.name.set("Laufmaschine")
        self.assertEqual(bike.pk, 1)


class IndexationTest(LimpydBaseTest):

    def test_stringfield_indexable(self):
        bike = Bike()
        bike.name.set("monocycle")
        self.assertFalse(Bike.exists(name="tricycle"))
        self.assertTrue(Bike.exists(name="monocycle"))
        bike.name.set("tricycle")
        self.assertFalse(Bike.exists(name="monocycle"))
        self.assertTrue(Bike.exists(name="tricycle"))


if __name__ == '__main__':
    unittest.main()
