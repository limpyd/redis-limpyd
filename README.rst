|PyPI Version| |Build Status|

======
Limpyd
======

`Limpyd` provides an **easy** way to store objects in `Redis <http://redis.io/>`_, **without losing the power and the control of the Redis API**, in a *limpid* way, with just as abstraction as needed.

Featuring:

- Don't care about keys, `limpyd` do it for you
- Retrieve objects from some of their attributes
- Retrieve objects collection
- CRUD abstraction
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

Python 2.6, 2.7, 3.3 and 3.4 are supported.

.. code:: bash

    pip install redis-limpyd


Documentation
=============

See https://redis-limpyd.readthedocs.org/ for a full documentation.


Maintainers
===========

* `Stéphane «Twidi» Angel <https://github.com/twidi/>`_
* `Yohan Boniface <https://github.com/yohanboniface/>`_


Extentions
==========

* A bundle of great extensions: `Limpyd-extensions <https://github.com/twidi/redis-limpyd-extensions>`_
* A queue/task/job manager: `Limpyd-jobs <https://github.com/twidi/redis-limpyd-jobs>`_

.. |PyPI Version| image:: https://pypip.in/v/redis-limpyd/badge.png
   :target: https://pypi.python.org/pypi/redis-limpyd
.. |Build Status| image:: https://travis-ci.org/yohanboniface/redis-limpyd.png
   :target: https://travis-ci.org/yohanboniface/redis-limpyd
