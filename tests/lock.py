# -*- coding:utf-8 -*-

import unittest

import threading
import time

from limpyd.utils import make_key
from limpyd import fields

from base import LimpydBaseTest
from model import TestRedisModel


class HookedStringField(fields.StringField):
    """
    Implement hooks for testing locks.
    """

    def _call_set(self, command, *args, **kwargs):
        _pre_callback = kwargs.pop('_pre_callback', None)
        _post_callback = kwargs.pop('_post_callback', None)
        if _pre_callback is not None:
            _pre_callback(command, *args, **kwargs)

        result = super(HookedStringField, self)._call_set(command, *args, **kwargs)

        if _post_callback is not None:
            _post_callback(result)

        return result


class Bike(TestRedisModel):
    namespace = "test-lock"
    name = HookedStringField(indexable=True)
    wheels = HookedStringField(default=2)
    passengers = HookedStringField(default=1)


class LockTest(LimpydBaseTest):

    class LockModelThread(threading.Thread):
        """
        Base class to use for creating threads Provide a communication system
        using redis pubsub to allow main thread and ones based on this to
        discuss (for example, at a given time, when ready, , tell the child
        thread to try to do some limpyd stuff).
        """

        # list of methods that can be called after a ping from another thread
        methods = []

        def __init__(self, test, model):
            self.test = test
            self.model = model
            super(LockTest.LockModelThread, self).__init__()

        def run(self):
            """
            Main method of the thread. Enter the wait loop. Each time a ping is
            received that is an existing method, we call this method. These
            methods must be defined in the "methods" attribute of the thread class.
            At the end, we send a ping to indicate the main thread that all
            work is done. This ping is sent when ALL the methods defined in
            "methods" are called.
            """

            def ping_callback(name):
                if hasattr(self, name) and callable(getattr(self, name)):
                    getattr(self, name)()

            LockTest.wait_for_ping(self.methods, ping_callback)
            LockTest.ping('thread_end')

        def test_lock(self, field_name, must_exists=True):
            """
            Test that a lock exists or not on the given field name.
            """
            lock_key = make_key(self.model._name, 'lock-for-update', field_name)
            method = self.test.assertTrue if must_exists else self.test.assertFalse
            method(LimpydBaseTest.database.connection.exists(lock_key))

    @staticmethod
    def ping(name):
        """
        Send a ping to the given channel name. It's a pubsub publish, used to
        communicate within threads. Use wait_for_ping below to receive pings
        and call a callback.
        """
        LimpydBaseTest.database.connection.publish(name, 1)

    @staticmethod
    def wait_for_ping(names, callback=None):
        """
        When a ping (see above) with a name in the given names is received, the
        callback is executed. We also stop listening for pings with the received
        name. As waiting is a blocking process (using redis pubsub subscribe),
        it can be used to call a callback or simply to wait for a ping to
        continue execution.
        """
        pubsub = LimpydBaseTest.database.connection.pubsub()
        pubsub.subscribe(names)
        for message in pubsub.listen():
            if message['type'] == 'message':
                if message['channel'] == 'exit':
                    pubsub.unsubscribe(names)
                    continue
                pubsub.unsubscribe(message['channel'])
                if callback:
                    callback(message['channel'])

    def test_two_updates_of_same_indexable_field_should_be_done_one_after_the_other(self):
        """
        Will test that when a lockable field is updated, an external update on
        the same field (in this instance or another instance) wait for the first
        update to be finished.
        """

        class BikeLockThread(LockTest.LockModelThread):
            methods = ['test_create_new_bike']

            def test_create_new_bike(self):
                """
                Create a new instance of the thread's model
                """
                # test that we have a lock
                self.test_lock('name')

                # create the instance (does nothing in redis)
                bike = self.model()

                def test_before_update(name, *args, **kwargs):
                    """
                    This function will be used as a pre_callback when setting a
                    new name of the thread's bike object. It will test that we
                    already have a bike in the collection.
                    """
                    self.test.assertEqual(len(self.model.collection()), 1)
                    return (args, kwargs)

                # set a name (will wait for lock to be released)
                bike.name.set('velocipede', _pre_callback=test_before_update)

        # start a new thread to work on the model Bike
        thread = BikeLockThread(self, Bike)
        thread.start()

        def ping_thread(name, *args, **kwargs):
            """
            This function will be used as a pre_callback when setting a new
            name of the local bike object. It send a ping telling the thread
            that it must create a new bike, with a name. As the name of a bike
            is indexable, a Lock will occurs, so the new bike in the thread
            should only be created after the name of the local bike is really
            saved and indexed.
            """
            time.sleep(0.1)  # wait for thread to be ready
            LockTest.ping('test_create_new_bike')
            time.sleep(0.1)  # to be sure the we can test lock in the thread
            return (args, kwargs)

        def test_end_update_local_bike(result):
            """
            This function will be used as a post_callback when setting a new
            name of the local bike object. It tests than, as we updated a
            lockable field, we only have the local bike in redis as for now, the
            one in the thread should have waited for the local save to be
            finished.
            """
            self.assertEqual(len(Bike.collection()), 1)
            return result

        # create a new bike (does nothing in redis)
        bike = Bike()
        # set a name to the bike and use pre/post callbacks to:
        # - ping the thread when the lock will be set
        # - test the thread did nothing just before the release of the lock
        bike.name.set('rosalie', _pre_callback=ping_thread,
                                 _post_callback=test_end_update_local_bike)

        # wait before thread exit
        if thread.is_alive():
            LockTest.wait_for_ping('thread_end', None)

        # now we should have the both bikes fully created
        self.assertEqual(len(Bike.collection()), 2)

    def test_two_updates_of_same_unlockable_indexable_field_should_be_done_without_lock(self):
        """
        Will test that when a unlockable field is updated, an external update on
        the same field (in this instance or another instance) doesn't wait for
        the first to be finished
        """

        class UnlockableBike(Bike):
            lockable = False

        class UnlockableBikeLockThread(LockTest.LockModelThread):
            methods = ['test_create_new_bike']

            def test_create_new_bike(self):
                """
                Create a new instance of the thread's model
                """

                # test that we have a lock
                self.test_lock('name', must_exists=False)

                # create the instance (does nothing in redis)
                bike = self.model()

                def test_before_update(name, *args, **kwargs):
                    """
                    This function will be used as a pre_callback when setting a
                    new name of the thread's bike object. It will test that we
                    doesn't have a bike yet in the collection.
                    """
                    self.test.assertEqual(len(self.model.collection()), 0)
                    return (args, kwargs)

                # set a name (will wait for lock to be released)
                bike.name.set('velocipede', _pre_callback=test_before_update)

        # start a new thread to work on the model Bike
        thread = UnlockableBikeLockThread(self, UnlockableBike)
        thread.start()

        def ping_thread(name, *args, **kwargs):
            """
            This function will be used as a pre_callback when setting a new
            name of the local bike object. It send a ping telling the thread
            that it must create a new bike, with a name. As the model is not
            lockable, no lock will be acquired, so the new bike in the thread
            should be created as soon as possible, without waiting for the local
            one to be saved.
            """
            time.sleep(0.1)  # wait for thread to be ready
            LockTest.ping('test_create_new_bike')
            time.sleep(0.1)  # to be sure the we can test no-lock in the thread
            return (args, kwargs)

        # create a new bike (does nothing in redis)
        bike = UnlockableBike()
        # set a name to the bike and use pre ping the thread before starting the
        # the update
        bike.name.set('rosalie', _pre_callback=ping_thread)

        # wait before thread exit
        LockTest.ping('exit')
        if thread.is_alive():
            LockTest.wait_for_ping('thread_end')

        # now we should have the both bikes fully created
        self.assertEqual(len(UnlockableBike.collection()), 2)


if __name__ == '__main__':
    unittest.main()
