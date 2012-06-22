# -*- coding:Utf-8 -*-

import unittest

from base import LimpydBaseTest
from limpyd.utils import make_key
from logging import getLogger, DEBUG, StreamHandler


class log_debug_to_stderr():
    log = getLogger('limpyd')
    log.setLevel(DEBUG)
    log.addHandler(StreamHandler())


class MakeKeyTest(LimpydBaseTest):

    def test_simple_key(self):
        self.assertEqual("simple_key", make_key("simple_key"))

    def test_multi_element_key(self):
        self.assertEqual("complex:key", make_key("complex", "key"))

    def test_unicode_element(self):
        self.assertEqual(u"french:key:clé", make_key("french", "key", u"clé"))

    def test_integer_element(self):
        self.assertEqual("integer:key:1", make_key("integer", "key", 1))


if __name__ == '__main__':
    unittest.main()
