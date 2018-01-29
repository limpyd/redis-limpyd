********
Database
********

The first element to define when using ``limpyd`` is the database. The main goal of the database is to handle the connection to Redis_ and to host the models.

It's easy to define a database, as its arguments are the same as for a standard connection to Redis_ via `redis-py`_:

.. code:: python

    from limpyd.database import RedisDatabase

    main_database = RedisDatabase(host='localhost', port=6379, db=0)


Then it's also easy to define the database (which is mandatory) on which a model is defined:

.. code:: python

    class Example(model.RedisModel):
        database = main_database
        some_field = fields.StringField()


If you have more than one model to host on a database, it's a good idea to create an abstract model:

.. code:: python

    class BaseModel(model.RedisModel):
        database = main_database
        abstract = True

    class Foo(BaseModel):
        foo_field = fields.StringField()

    class Bar(BaseModel):
        bar_field = fields.StringField()


Note that you cannot have two models with the same name (the name of the class) in the same database (for obvious collision problems), but we provide a namespace attribute on models to solve this issue (so you can use an external module with models named as yours). See :mod:`~models` to know how to use them.

It's not a good idea to declare many ``RedisDatabase`` objects on the same Redis_ database (defined with ``host``+``port``+``db``), because of obvious collision problems if models have the same name in each. So do it only if you really know what you're doing, and with different models only.

Sometimes you may want to change the database used after the models are created. It can be useful if you want to use models defined in an external module. To manage this, simply use the ``use_database`` method of a model class.

Say you use an external module defined like this:

.. code:: python

    class BaseModel(RedisModel):
        database = RedisDatabase()
        abstract = True

    class Foo(BaseModel):
        # ... fields ...

    class Bar(BaseModel):
        # ... fields ...

In your code, to add these models to your database (which also allow to use them in :ref:`Related model <RelatedModel>`), simply do:

.. code:: python

    database = RedisDatabase(**connection_settings)
    BaseModel.use_database(database)

You can notice that you don't have to call this method on ``Foo`` and ``Bar``. It's because they are subclasses of ``BaseModel`` and they don't have another database defined.

If you simply want to change the settings of the Redis_ connection to use (different server or db), you can use the ``connect`` method of your database, which accepts the same parameters as the constructor:

.. code:: python

    main_database = RedisDatabase(host='localhost', port=6379, db=0)

    # ... later ...

    main_database.connect(host='localhost', port=6370, db=3)


.. _Redis: http://redis.io
.. _redis-py: https://github.com/andymccurdy/redis-py
