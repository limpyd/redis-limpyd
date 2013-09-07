# -*- coding:Utf-8 -*-

import sys
if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from base import LimpydBaseTest
from limpyd.utils import make_key, unique_key


class MakeKeyTest(LimpydBaseTest):

    def test_simple_key(self):
        self.assertEqual("simple_key", make_key("simple_key"))

    def test_multi_element_key(self):
        self.assertEqual("complex:key", make_key("complex", "key"))

    def test_unicode_element(self):
        self.assertEqual(u"french:key:clé", make_key("french", "key", u"clé"))

    def test_integer_element(self):
        self.assertEqual("integer:key:1", make_key("integer", "key", 1))


class UniqueKeyTest(LimpydBaseTest):

    def test_generated_key_must_be_a_string(self):
        key = unique_key(self.connection)
        self.assertEqual(type(key), str)

    def test_generated_key_must_be_unique(self):
        key1 = unique_key(self.connection)
        key2 = unique_key(self.connection)
        self.assertNotEqual(key1, key2)


class LimpydBaseTestTest(LimpydBaseTest):
    """
    Test parts of LimpydBaseTest
    """

    def test_assert_num_commands_is_ok(self):
        with self.assertNumCommands(1):
            # we know that info do only one command
            self.connection.info()


if __name__ == '__main__':
    unittest.main()
