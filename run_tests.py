#!/usr/bin/env python

import unittest
import argparse

# FIXME: move tests in limpyd module, to prevent a relative import?
from tests import base, model, utils, collection
from tests.contrib import database


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
        default_mods = [base, model, utils, collection, ]
        contrib_mods = [database, ]
        for mod in default_mods + contrib_mods:
            suite = unittest.TestLoader().loadTestsFromModule(mod)
            suites.append(suite)
        suite = unittest.TestSuite(suites)
    unittest.TextTestRunner(verbosity=args.verbosity).run(suite)
