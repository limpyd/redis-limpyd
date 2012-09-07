# -*- coding:utf-8 -*-

import unittest

from limpyd import fields
from limpyd.contrib.collection import ExtendedCollectionManager
from limpyd.utils import unique_key
from limpyd.exceptions import *

from ..base import LimpydBaseTest, test_database
from ..model import TestRedisModel


class Group(TestRedisModel):
    namespace = 'contrib-collection'
    collection_manager = ExtendedCollectionManager

    id = fields.AutoPKField()
    name = fields.HashableField(indexable=True)
    active = fields.HashableField(indexable=True, default=1)
    public = fields.HashableField(indexable=True, default=1)


class GroupsContainer(TestRedisModel):
    namespace = 'contrib-collection'
    groups_set = fields.SetField()
    groups_list = fields.ListField()
    groups_sortedset = fields.SortedSetField()


class BaseTest(LimpydBaseTest):

    def setUp(self):
        super(BaseTest, self).setUp()
        self.groups = [
            Group(name='foo'),
            Group(name='bar', public=0),
            Group(name='baz', active=0),
            Group(name='qux', active=0, public=0),
        ]


class CompatibilityTest(BaseTest):

    def test_extended_collection_should_work_as_simple_one(self):

        # test "all"
        all_pks = set(Group.collection())
        self.assertEqual(all_pks, set(['1', '2', '3', '4']))

        # test "sort by"
        all_pks_by_name = list(Group.collection().sort(by='name', alpha=True))
        self.assertEqual(all_pks_by_name, ['2', '3', '1', '4'])

        # test "filter"
        active_pks = set(Group.collection(active=1))
        self.assertEqual(active_pks, set(['1', '2']))
        first_group_pk = list(Group.collection(pk=1))
        self.assertEqual(first_group_pk, ['1', ])
        bad_groups = list(Group.collection(pk=10, active=1))
        self.assertEqual(bad_groups, [])

        # test "instances"
        public_groups = list(Group.collection(public=1).instances())
        self.assertEqual(len(public_groups), 2)
        public_groups_pks = set([g.get_pk() for g in public_groups])
        self.assertEqual(public_groups_pks, set(['1', '3']))

        # test "values"
        active_public_dicts = list(Group.collection(public=1, active=1).values('name', 'active', 'public'))
        self.assertEqual(len(active_public_dicts), 1)
        self.assertEqual(active_public_dicts[0], {'name': 'foo', 'active': '1', 'public': '1'})

        # test "values_list"
        active_public_tuples = list(Group.collection(public=1, active=1).values_list('name', 'active', 'public'))
        self.assertEqual(len(active_public_tuples), 1)
        self.assertEqual(active_public_tuples[0], ('foo', '1', '1'))
        active_names = list(Group.collection(active=1).values_list('name', flat=True).sort(by='name', alpha=True))
        self.assertEqual(len(active_names), 2)
        self.assertEqual(active_names, ['bar', 'foo'])


class FilterTest(BaseTest):

    def test_filter_method_should_add_filter(self):
        # test with one call
        collection = Group.collection(active=1).filter(public=1)
        self.assertEqual(set(collection), set(['1']))
        # test with two calls
        collection = Group.collection(active=1)
        self.assertEqual(set(collection), set(['1', '2']))
        collection.filter(public=1)
        self.assertEqual(set(collection), set(['1']))
        # test with a pk
        collection = Group.collection(active=1).filter(pk=2)
        self.assertEqual(set(collection), set(['2']))
        collection = Group.collection(active=1).filter(id=1)
        self.assertEqual(set(collection), set(['1']))
        collection = Group.collection(active=1).filter(id=10)
        self.assertEqual(set(collection), set())
        # test with pk, then filter with other
        collection = Group.collection(pk=2).filter(active=1)
        self.assertEqual(set(collection), set(['2']))

    def test_filter_calls_could_be_chained(self):
        collection = Group.collection().filter(active=1).filter(public=1).filter(pk=1)
        self.assertEqual(set(collection), set(['1']))

    def test_redefining_filter_should_return_empty_result(self):
        collection = Group.collection(active=1).filter(active=0)
        self.assertEqual(set(collection), set())

    def test_filter_should_returns_the_collection(self):
        collection1 = Group.collection(active=1)
        collection2 = collection1.filter(active=0)
        self.assertEqual(collection1, collection2)

    def test_filter_should_accept_pks(self):
        collection = Group.collection(pk=1)
        self.assertEqual(set(collection), set(['1']))
        collection.filter(id=1)
        self.assertEqual(set(collection), set(['1']))
        collection.filter(pk=2)
        self.assertEqual(set(collection), set())


class IntersectTest(BaseTest):

    redis_zinterstore = None
    redis_sinterstore = None

    @staticmethod
    def zinterstore(*args, **kwargs):
        """
        Store arguments and call the real zinterstore command
        """
        IntersectTest.last_interstore_call = {
            'command': 'zinterstore',
            'sets': args[1]
        }
        return IntersectTest.redis_zinterstore(*args, **kwargs)

    @staticmethod
    def sinterstore(*args, **kwargs):
        """
        Store arguments and call the real sinterstore command
        """
        IntersectTest.last_interstore_call = {
            'command': 'sinterstore',
            'sets': args[1]
        }
        return IntersectTest.redis_sinterstore(*args, **kwargs)

    def setUp(self):
        """
        Update the redis zinterstore and sinterstore commands to be able to
        store locally arguments for testing them just after the commands are
        called. Store the original command to call it after logging, and to
        restore it in tearDown.
        """
        super(IntersectTest, self).setUp()
        IntersectTest.last_interstore_call = {'command': None, 'sets': [], }
        IntersectTest.redis_zinterstore = self.connection.zinterstore
        self.connection.zinterstore = IntersectTest.zinterstore
        IntersectTest.redis_sinterstore = self.connection.sinterstore
        self.connection.sinterstore = IntersectTest.sinterstore

    def tearDown(self):
        """
        Restore the zinterstore and sinterstore previously updated in setUp.
        """
        self.connection.zinterstore = IntersectTest.redis_zinterstore
        self.connection.sinterstore = IntersectTest.redis_sinterstore
        super(IntersectTest, self).tearDown()

    def test_intersect_should_accept_string(self):
        set_key = unique_key(self.connection)
        self.connection.sadd(set_key, 1, 2)
        collection = set(Group.collection().intersect(set_key))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        set_key = unique_key(self.connection)
        self.connection.sadd(set_key, 1, 2, 10, 50)
        collection = set(Group.collection().intersect(set_key))
        self.assertEqual(collection, set(['1', '2']))

    def test_intersect_should_accept_set(self):
        collection = set(Group.collection().intersect(set([1, 2])))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        collection = set(Group.collection().intersect(set([1, 2, 10, 50])))
        self.assertEqual(collection, set(['1', '2']))

    def test_intersect_should_accept_list(self):
        collection = set(Group.collection().intersect([1, 2]))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        collection = set(Group.collection().intersect([1, 2, 10, 50]))
        self.assertEqual(collection, set(['1', '2']))

    def test_intersect_should_accept_tuple(self):
        collection = set(Group.collection().intersect((1, 2)))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        collection = set(Group.collection().intersect((1, 2, 10, 50)))
        self.assertEqual(collection, set(['1', '2']))

    def test_intersect_should_accept_setfield(self):
        container = GroupsContainer()

        container.groups_set.sadd(1, 2)
        collection = set(Group.collection().intersect(container.groups_set))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        container.groups_set.sadd(10, 50)
        collection = set(Group.collection().intersect(container.groups_set))
        self.assertEqual(collection, set(['1', '2']))

    def test_intersect_should_accept_listfield_without_scripting(self):
        container = GroupsContainer()

        container.groups_list.lpush(1, 2)
        collection = set(Group.collection().intersect(container.groups_list))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        container.groups_list.lpush(10, 50)
        collection = set(Group.collection().intersect(container.groups_list))
        self.assertEqual(collection, set(['1', '2']))

    @unittest.skipUnless(test_database.has_scripting(), "Redis scripting not available")
    def test_intersect_should_accept_listfield_via_scripting(self):
        container = GroupsContainer()

        container.groups_list.lpush(1, 2)
        collection = set(Group.collection().intersect(container.groups_list))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        self.assertEqual(collection, set(['1', '2']))

        container.groups_list.lpush(10, 50)
        collection = set(Group.collection().intersect(container.groups_list))
        self.assertEqual(collection, set(['1', '2']))

    def test_intersect_should_accept_sortedsetfield(self):
        container = GroupsContainer()

        container.groups_sortedset.zadd(1.0, 1, 2.0, 2)
        collection = set(Group.collection().intersect(container.groups_sortedset))
        self.assertEqual(collection, set(['1', '2']))

        container.groups_sortedset.zadd(10.0, 10, 50.0, 50)
        collection = set(Group.collection().intersect(container.groups_sortedset))
        self.assertEqual(collection, set(['1', '2']))

    def test_passing_sortedset_in_intersect_use_zinterstore(self):
        container = GroupsContainer()
        container.groups_sortedset.zadd(1.0, 1, 2.0, 2)
        collection = Group.collection().intersect(container.groups_sortedset)

        # execute the collection
        result = list(collection)
        # check that we called an interstore
        self.assertEqual(self.last_interstore_call['command'], 'zinterstore')
        # check the intersection is done with the sorted set AND the whole
        # collection, because we have no filters
        self.assertIn(Group._redis_attr_pk.collection_key,
                      self.last_interstore_call['sets'])
        self.assertEqual(result, ['1', '2'])

        # add a filter to the collection
        collection.filter(public=1)
        # execute the collection
        result = list(collection)
        # check that we called an interstore
        self.assertEqual(self.last_interstore_call['command'], 'zinterstore')
        # check the intersection is not done with the whole collection, but
        # only the sorted set and the set from the filter
        self.assertNotIn(Group._redis_attr_pk.collection_key,
                         self.last_interstore_call['sets'])
        self.assertEqual(result, ['1', ])

    def test_intersect_should_raise_if_unsupported_type(self):
        # unsupported type
        with self.assertRaises(ValueError):
            Group.collection().intersect({})
        # unbound MultiValuesField
        with self.assertRaises(ValueError):
            Group.collection().intersect(GroupsContainer._redis_attr_groups_set)
        with self.assertRaises(ValueError):
            Group.collection().intersect(GroupsContainer._redis_attr_groups_list)
        with self.assertRaises(ValueError):
            Group.collection().intersect(GroupsContainer._redis_attr_groups_sortedset)

    def test_intersect_can_be_called_many_times(self):
        collection = set(Group.collection().intersect([1, 2, 3, 10]).intersect([2, 3, 50]))
        self.assertEqual(collection, set(['2', '3']))

    def test_intersect_can_be_called_with_filter(self):
        collection = Group.collection(active=1).filter(public=1).intersect([1, 2, 3, 10])
        self.assertEqual(set(collection), set(['1']))
        self.assertEqual(self.last_interstore_call['command'], 'sinterstore')
        collection = collection.intersect([2, 3, 50])
        self.assertEqual(set(collection), set())

    def test_intersect_should_returns_the_collection(self):
        collection1 = Group.collection(active=1)
        collection2 = collection1.intersect([1, 2])
        self.assertEqual(collection1, collection2)
