# -*- coding:utf-8 -*-

# Add the tests main directory into the path, to be able to load things from base
import os
import sys
sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(__file__), '..')))

import unittest

from limpyd import fields
from limpyd.contrib.related import (RelatedModel, RelatedCollection,
                                    FKStringField, FKHashableField, M2MSetField,
                                    M2MListField, M2MSortedSetField)

from base import LimpydBaseTest


class TestRedisModel(RelatedModel):
    """
    Use it as a base for all RelatedModel created for tests
    """
    database = LimpydBaseTest.database
    abstract = True
    namespace = "related-tests"


class Person(TestRedisModel):

    name = fields.PKField()

    owned_groups = RelatedCollection('Group', 'owner')
    membership = RelatedCollection('Group', 'members')


class Group(TestRedisModel):
    name = fields.PKField()
    owner = FKHashableField()
    parent = FKStringField()
    members = M2MSetField()

    children = RelatedCollection('Group', 'parent')


class FKTest(LimpydBaseTest):

    def test_can_access_reverse_fk(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon._pk)
        self.assertEqual(set(Group.collection(owner=ybon._pk)), set([core_devs._pk]))
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))

    def test_fk_can_be_given_as_object(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        self.assertEqual(core_devs.owner.hget(), ybon._pk)
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))

    def test_can_update_fk(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')
        twidi = Person(name='twidi')

        core_devs.owner.hset(ybon)
        self.assertEqual(set(ybon.owned_groups()), set([core_devs._pk]))

        core_devs.owner.hset(twidi)
        self.assertEqual(set(ybon.owned_groups()), set())
        self.assertEqual(set(twidi.owned_groups()), set([core_devs._pk]))

    def test_many_fk_can_be_set_on_same_object(self):
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')
        twidi = Person(name='twidi')

        core_devs.owner.hset(twidi)
        fan_boys.owner.hset(twidi)
        self.assertEqual(set(twidi.owned_groups()), set([core_devs._pk, fan_boys._pk]))

    def test_fk_can_be_set_on_same_model(self):
        main_group = Group(name='limpyd groups')
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')

        core_devs.parent.set(main_group)
        fan_boys.parent.set(main_group)
        self.assertEqual(set(main_group.children()), set([core_devs._pk, fan_boys._pk]))

    def test_deleting_an_object_must_clear_the_fk(self):
        main_group = Group(name='limpyd groups')
        core_devs = Group(name='limpyd core devs')
        fan_boys = Group(name='limpyd fan boys')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        ybon.delete()
        self.assertIsNone(core_devs.owner.hget())

        core_devs.parent.set(main_group)
        fan_boys.parent.set(main_group)
        main_group.delete()
        self.assertIsNone(core_devs.parent.get())
        self.assertIsNone(fan_boys.parent.get())

    def test_deleting_a_fk_must_clean_the_collection(self):
        core_devs = Group(name='limpyd core devs')
        ybon = Person(name='ybon')

        core_devs.owner.hset(ybon)
        core_devs.delete()
        self.assertEqual(set(ybon.owned_groups()), set())


class M2MSetTest(LimpydBaseTest):
    pass


class M2MListTest(LimpydBaseTest):

    class Group2(TestRedisModel):
        members = M2MListField()


class M2MSortedSetTest(LimpydBaseTest):

    class Group3(TestRedisModel):
        members = M2MSortedSetField()


if __name__ == '__main__':
    unittest.main()
