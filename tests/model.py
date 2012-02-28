import unittest

# HACK
import sys, os
sys.path.insert(0, os.getcwd())

from limpyd.model import Bike
from base import LimpydBaseTest

class CreationTest(LimpydBaseTest):

    def test_dummy(self):
        bike = Bike()
        bike.name.set("Laufmaschine")
        self.assertEqual(bike.pk, 1)


if __name__ == '__main__':
    unittest.main()
