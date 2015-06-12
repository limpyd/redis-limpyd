#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
from pip.req import parse_requirements
from setuptools import setup, find_packages
import sys

import limpyd

# The `session` argument for the `parse_requirements` function is available (but
# optional) in pip 1.5, and mandatory in next versions
try:
    from pip.download import PipSession
except ImportError:
    parse_args = {}
else:
    parse_args = {'session': PipSession()}


def get_requirements(source):
    install_reqs = parse_requirements(source, **parse_args)
    return set([str(ir.req) for ir in install_reqs])


if sys.version_info >= (2, 7):
    install_requires = get_requirements('requirements.txt'),
else:
    install_requires = get_requirements('requirements-2.6.txt'),


long_description = codecs.open('README.rst', "r", "utf-8").read()

setup(
    name = "redis-limpyd",
    version = limpyd.__version__,
    author = limpyd.__author__,
    author_email = limpyd.__contact__,
    description = limpyd.__doc__,
    keywords = "redis",
    url = limpyd.__homepage__,
    download_url = "https://github.com/yohanboniface/redis-limpyd/tags",
    packages = find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    install_requires=install_requires,
    platforms=["any"],
    zip_safe=True,

    long_description = long_description,

    classifiers = [
        "Development Status :: 3 - Alpha",
        #"Environment :: Web Environment",
        "Intended Audience :: Developers",
        #"License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
    ],
)
