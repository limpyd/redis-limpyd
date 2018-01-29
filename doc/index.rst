Welcome to redis-limpyd's documentation!
========================================

``Limpyd`` provides an **easy** way to store objects in `Redis <http://redis.io/>`_, **without losing the power and the control of the Redis API**, in a *limpid* way, with just as abstraction as needed.

Featuring:

- Don't care about keys, ``limpyd`` do it for you
- Retrieve objects from some of their attributes
- Retrieve objects collection
- CRUD abstraction
- Powerful indexing and filtering
- Keep the power of all the `Redis data types <http://redis.io/topics/data-types>`_ in your own code

Source code: https://github.com/limpyd/redis-limpyd

|PyPI Version| |Build Status| |Doc Status|

Show me some code!
------------------

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


Contents
========

.. toctree::
   :maxdepth: 6

   about
   database
   models
   fields
   collections
   contrib



.. |PyPI Version| image:: https://img.shields.io/pypi/v/redis-limpyd.png
   :target: https://pypi.python.org/pypi/redis-limpyd
.. |Build Status| image:: https://travis-ci.org/limpyd/redis-limpyd.png
   :target: https://travis-ci.org/limpyd/redis-limpyd
.. |Doc Status| image:: https://readthedocs.org/projects/redis-limpyd/badge/
   :target: http://redis-limpyd.readthedocs.io/en/latest/
