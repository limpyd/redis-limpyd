# -*- coding:utf-8 -*-
from __future__ import unicode_literals

import threading
import time

from limpyd.contrib.database import PipelineDatabase, _Pipeline
from limpyd import model, fields

from ..base import LimpydBaseTest, TEST_CONNECTION_SETTINGS

test_database = PipelineDatabase(**TEST_CONNECTION_SETTINGS)


class Bike(model.RedisModel):
    database = test_database
    namespace = 'database-contrib-tests'

    name = fields.StringField(indexable=True)
    wheels = fields.StringField(default=2)
    passengers = fields.StringField(default=1)


class PipelineTest(LimpydBaseTest):
    database = test_database

    def test_simple_pipeline_without_transaction(self):
        bike = Bike(name="rosalie", wheels=4)
        bike2 = Bike(name="velocipede")
        self.assertNotIsInstance(self.database.connection, _Pipeline)
        with self.database.pipeline(transaction=False) as pipe:
            self.assertIsInstance(self.database.connection, _Pipeline)
            bike.name.get()
            bike2.name.get()
            names = pipe.execute()
        self.assertEqual(names, ["rosalie", "velocipede"])
        self.assertNotIsInstance(self.database.connection, _Pipeline)

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

    def test_pipeline_should_be_for_current_thread_only(self):
        bike = Bike(name="rosalie", wheels=4)
        bike2 = Bike(name="velocipede")

        class Thread(threading.Thread):
            def __init__(self, test):
                self.test = test
                super(Thread, self).__init__()

            def run(self):
                # in the thread we should have a direct connection
                self.test.assertNotIsInstance(self.test.database.connection, _Pipeline)
                # check it by getting a value
                self.test.assertEqual(bike2.name.get(), 'velocipede')

        with self.database.pipeline(transaction=False) as pipe:
            bike.name.get()
            bike2.name.get()

            thread = Thread(self)
            thread.start()

            # wait a little to let the child thread do its tests
            time.sleep(0.2)

            names = pipe.execute()
            self.assertEqual(names, ["rosalie", "velocipede"])  # two in the pipeline

    def test_pipeline_could_be_shared(self):
        bike = Bike(name="rosalie", wheels=4)
        bike2 = Bike(name="velocipede")

        class Thread(threading.Thread):
            def __init__(self, test):
                self.test = test
                super(Thread, self).__init__()

            def run(self):
                # in the thread we should have the pipelined connection
                self.test.assertIsInstance(self.test.database.connection, _Pipeline)
                # check it by asking a value to the pileine, getting back the pipeline
                self.test.assertIsInstance(bike2.name.get(), _Pipeline)

        with self.database.pipeline(transaction=False, share_in_threads=True) as pipe:
            bike.name.get()
            bike2.name.get()

            thread = Thread(self)
            thread.start()

            # wait a little to let the child thread do its tests
            time.sleep(0.2)

            names = pipe.execute()
            self.assertEqual(names, ["rosalie", "velocipede", "velocipede"])  # trhee in the pipeline, with one from the thread
