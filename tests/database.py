# -*- coding:utf-8 -*-

import unittest

from limpyd import model, fields
from limpyd.exceptions import *

from base import LimpydBaseTest
from model import Bike, TestRedisModel


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


class PipelineTest(LimpydBaseTest):

    def test_simple_pipeline_without_transaction(self):
        bike = Bike(name="rosalie", wheels=4)
        bike2 = Bike(name="velocipede")
        with self.database.pipeline(transaction=False) as pipe:
            bike.name.get()
            bike2.name.get()
            names = pipe.execute()
        self.assertEqual(names, ["rosalie", "velocipede"])

    def test_transaction_method(self):
        bike = Bike(name="rosalie", wheels=4)
        bike2 = Bike(name="velocipede")

        # function to run in a transaction with watched keys
        def do_stuff(pipeline):
            do_stuff.counter += 1

            # update the watched key to generate a WatchError and restart the
            # transaction
            if int(bike2.wheels.get()) != 10:
                bike2.wheels.set(10)

            # pass in transaction mode
            pipeline.multi()

            # get wheels for the two bikes
            bike.wheels.set(0)
            bike.wheels.get()
            bike2.wheels.get()

        # count how much time we enter the do_stuff method
        do_stuff.counter = 0

        # start the transaction with a watched key
        result = self.database.transaction(do_stuff, *[bike2.wheels])

        # True for the set, 0 and 10 for the two get
        self.assertEqual(result, [True, "0", "10"])

        # we entered the function two times because a watched key was updated
        self.assertEqual(do_stuff.counter, 2)


if __name__ == '__main__':
    unittest.main()
