#!/usr/bin/env python
from __future__ import unicode_literals

import argparse
import importlib
import os
import sys
if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

import tests


if __name__ == "__main__":

    # Define arguments
    parser = argparse.ArgumentParser(description="Run redis-limpyd tests suite.")
    parser.add_argument(
        "tests",
        nargs="*",
        default=None,
        help="Tests (module, TestCase or TestCaseMethod) to run. "
             "use full path, eg.: `tests.module.Class.test` ; "
             "`tests.module.Class` works also ; `tests.module` too!"
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        type=int,
        action="store",
        dest="verbosity",
        default=2,
        help="Verbosity of the runner."
    )
    args = parser.parse_args()

    if args.tests:
        # we have names
        suite = unittest.TestLoader().loadTestsFromNames(args.tests)
    else:
        # Run all the tests

        suites = []

        tests_folder = os.path.dirname(tests.__file__)
        for root, dirs, files in os.walk(tests_folder):
            for file in files:
                if not file.endswith('.py') or file == '__init__.py':
                    continue
                rel_path = os.path.relpath(os.path.join(root, file), start=tests_folder)
                module_name = 'tests.' + rel_path.replace('/', '.').replace('\\', '.')[:-3]
                module = importlib.import_module(module_name)
                suite = unittest.TestLoader().loadTestsFromModule(module)
                suites.append(suite)

        suite = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=args.verbosity).run(suite)
