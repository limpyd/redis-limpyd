# -*- coding:utf-8 -*-

from limpyd import fields

from ..base import LimpydBaseTest
from ..model import TestRedisModel


class HashFieldTest(LimpydBaseTest):

    class EmailTestModel(TestRedisModel):
        headers = fields.HashField(indexable=True)
        raw_headers = fields.HashField()

    def test_hmset_should_set_values(self):
        obj = self.EmailTestModel()
        headers = {
            'from': 'foo@bar.com',
            'to': 'me@world.org'
        }
        obj.headers.hmset(**headers)
        self.assertEqual(obj.headers.hget('from'), 'foo@bar.com')
        self.assertEqual(obj.headers.hget('to'), 'me@world.org')

    def test_hmset_should_be_indexable(self):
        obj = self.EmailTestModel()
        obj.headers.hmset(**{'from': 'you@moon.io'})
        self.assertEqual(set(self.EmailTestModel.collection(headers__from='you@moon.io')), set([obj._pk]))

        # Now change value and check first has been deindexed and new redindexed
        obj.headers.hmset(**{'from': 'you@mars.io'})
        self.assertEqual(set(self.EmailTestModel.collection(headers__from='you@moon.io')), set())
        self.assertEqual(set(self.EmailTestModel.collection(headers__from='you@mars.io')), set([obj._pk]))

    def test_hset_should_set_value_and_be_indexable(self):
        obj = self.EmailTestModel()
        obj.headers.hset('from', 'someone@cassini.io')
        self.assertEqual(obj.headers.hget('from'), 'someone@cassini.io')

        self.assertEqual(set(self.EmailTestModel.collection(headers__from='someone@cassini.io')), set([obj._pk]))

        # Now change value and check first has been deindexed and new redindexed
        obj.headers.hset('from', 'someoneelse@cassini.io')
        self.assertEqual(set(self.EmailTestModel.collection(headers__from='someone@cassini.io')), set())
        self.assertEqual(set(self.EmailTestModel.collection(headers__from='someoneelse@cassini.io')), set([obj._pk]))

    def test_hincrby_should_set_value_and_be_indexable(self):
        obj = self.EmailTestModel()
        obj.headers.hincrby('Message-ID', 1)
        self.assertEqual(obj.headers.hget('Message-ID'), '1')
        self.assertEqual(set(self.EmailTestModel.collection(**{'headers__Message-ID': '1'})), set([obj._pk]))
        # Now change value and check first has been deindexed and new redindexed
        obj.headers.hincrby('Message-ID', 1)
        self.assertEqual(obj.headers.hget('Message-ID'), '2')
        self.assertEqual(set(self.EmailTestModel.collection(**{'headers__Message-ID': '1'})), set())
        self.assertEqual(set(self.EmailTestModel.collection(**{'headers__Message-ID': '2'})), set([obj._pk]))

    def test_delete_hashfield(self):
        obj = self.EmailTestModel()
        headers = {
            'from': 'foo@bar.com',
            'to': 'me@world.org'
        }
        obj.headers.hmset(**headers)
        self.assertEqual(obj.headers.hget('from'), 'foo@bar.com')
        self.assertEqual(obj.headers.hget('to'), 'me@world.org')
        obj.headers.hdel('from')
        self.assertEqual(obj.headers.hget('from'), None)
        self.assertEqual(set(self.EmailTestModel.collection(headers__from='foo@bar.com')), set())

        # Do not raise if we try to del a key that does not exist
        # (follow redis usage)
        obj.headers.hdel('a key that does not exist')

    def test_hsetnx_should_index_only_if_value_is_new(self):
        obj = self.EmailTestModel()
        obj.headers.hset('to', 'two@three.org')
        with self.assertNumCommands(5):
            # three calls for lock
            # one for setting value
            # one for indexing
            obj.headers.hsetnx('from', 'one@two.org')

        # Chech value has been changed
        self.assertEqual(obj.headers.hget('from'), 'one@two.org')

        with self.assertNumCommands(4):
            # three calls for lock
            # one for hsetnx, which should not set
            # one for indexing
            obj.headers.hsetnx('from', 'three@four.org')

        # Chech value has not been changed
        self.assertEqual(obj.headers.hget('from'), 'one@two.org')

    def test_hgetall_should_return_a_dict(self):
        obj = self.EmailTestModel()
        headers = {
            'from': 'foo@bar.com',
            'to': 'me@world.org'
        }
        obj.headers.hmset(**headers)
        self.assertEqual(
            obj.headers.hgetall(),
            headers
        )
