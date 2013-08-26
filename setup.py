#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs

from setuptools import setup

import limpyd

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
    packages = ['limpyd'],
    include_package_data=True,
    install_requires=["redis", ],
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
    ],
)

