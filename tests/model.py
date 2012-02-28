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


if __name__ == '__main__':
    unittest.main()
