#!/usr/bin/env python
# -*- coding: utf-8 -*-

import codecs
from pip.req import parse_requirements
from setuptools import setup, find_packages
import sys

import limpyd


def get_requirements(source):
    """
    Get the path of a requirements file and return a dict with:
      - `packages`: the list of all packages to install, in the format `name==version`
                    to be used in the `install_requires` argument of setup()
      - `links`: a list of urls to use as links in the `dependency_links` argument
                 of setup(), in th format `url#egg=name-version, BUT in the
                 requirements file, the link MUST be set in the format `url#egg=name==version`
                 (note the `==` required in the requirements file.)
                 The == is used to get the package name+version to put in `packages`,
                 but to process the dependency, pip expect a `-`, not `==`

    """
    install_reqs = list(parse_requirements(source))
    return {
        'packages': [str(ir.req) for ir in install_reqs],
        'links': ['%s#egg=%s' % (ir.url, str(ir.req).replace('==', '-')) for ir in install_reqs if ir.url],
    }


if sys.version_info >= (2, 7):
    requirements = get_requirements('requirements.txt')
else:
    requirements = get_requirements('requirements-2.6.txt')


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
    install_requires=requirements['packages'],
    dependency_links=requirements['links'],
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
