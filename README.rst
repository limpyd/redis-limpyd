|PyPI Version| |Build Status| |Doc Status|

======
Limpyd
======

``Limpyd`` provides an **easy** way to store objects in Redis_, **without losing the power and the control of the Redis API**, in a *limpid* way, with just as abstraction as needed.

Featuring:

- Don't care about keys, ``limpyd`` do it for you
- Retrieve objects from some of their attributes
- Retrieve objects collection
- CRUD abstraction
- Powerful indexing and filtering
- Keep the power of all the `Redis data types <http://redis.io/topics/data-types>`_ in your own code

Example of configuration:

.. code:: python

    from limpyd import model

    main_database = model.RedisDatabase(
        host="localhost",
        port=6379,
        db=0
    )

    class Bike(model.RedisModel):

        database = main_database

        name = model.InstanceHashField(indexable=True, unique=True)
        color = model.InstanceHashField()
        wheels = model.StringField(default=2)


So you can use it like this:

.. code:: python

    >>> mountainbike = Bike(name="mountainbike")
    >>> mountainbike.wheels.get()
    '2'
    >>> mountainbike.wheels.incr()
    >>> mountainbike.wheels.get()
    '3'
    >>> mountainbike.name.set("tricycle")
    >>> tricycle = Bike.collection(name="tricycle")[0]
    >>> tricycle.wheels.get()
    '3'
    >>> tricycle.hmset(color="blue")
    True
    >>> tricycle.hmget('color')
    ['blue']
    >>> tricycle.hmget('color', 'name')
    ['blue', 'tricycle']
    >>> tricycle.color.hget()
    'blue'
    >>> tricycle.color.hset('yellow')
    True
    >>> tricycle.hmget('color')
    ['yellow']


Install
=======

Python versions ``2.7`` and ``3.4`` to ``3.6`` are supported (CPython and PyPy).

Redis-py_ versions ``>= 2.9.1`` and ``< 2.11`` are supported.

.. code:: bash

    pip install redis-limpyd


Note: Version 1.0, 1.0.1 and 1.1 where broken so removed from PyPI


Documentation
=============

See https://redis-limpyd.readthedocs.io/ for a full documentation

Changelog
=========

See `CHANGELOG.rst <CHANGELOG.rst>`_

Maintainers
===========

* `Stéphane «Twidi» Angel <https://github.com/twidi/>`_
* `Yohan Boniface <https://github.com/yohanboniface/>`_


Extensions
==========

* A bundle of great extensions: `Limpyd-extensions <https://github.com/limpyd/redis-limpyd-extensions>`_
* A queue/task/job manager: `Limpyd-jobs <https://github.com/limpyd/redis-limpyd-jobs>`_

.. |PyPI Version| image:: https://img.shields.io/pypi/v/redis-limpyd.png
   :target: https://pypi.python.org/pypi/redis-limpyd
.. |Build Status| image:: https://travis-ci.org/limpyd/redis-limpyd.png?branch=master
   :target: https://travis-ci.org/limpyd/redis-limpyd
.. |Doc Status| image:: https://readthedocs.org/projects/redis-limpyd/badge/
   :target: http://redis-limpyd.readthedocs.io/en/latest/
.. _Redis: http://redis.io
.. _Redis-py: https://github.com/andymccurdy/redis-py
