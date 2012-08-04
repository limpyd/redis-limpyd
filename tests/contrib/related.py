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
    pass


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
