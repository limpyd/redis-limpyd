******
Models
******

:doc:`models` are the core of limpyd, it's why we're here. A `RedisModel` is a class, in a database, with some fields. Each instance of this model is a new object stored in Redis_ by `limpyd`.

Here a simple example::

    class Example(model.RedisModel):
        database = main_database

        foo = field.StringField()
        bar = field.StringField()

To create an instance, it's as easy as::

    >>> example = Example(foo='FOO', bar='BAR')

By just doing this, the fields are created, and a :ref:`PKField` is set with a value that you can use::

    >>> print "New example object with pk #%s" % example.pk.get()
    New example object with pk #1

Then later to get an instance from Redis_ with it's pk, it's as simple as::

    >>> example = Example(1)

So, to create an object, pass fields and their values as named arguments, and to retrieve it, pass its pk as the only argument. To retrieave instances via other fields than the pk, check the :doc:`collections` section later in this document.

If you don't pass any argument to the `RedisModel`, default one from fields are taken and are saved. But if no arguments and no default values, you get an empty instance, with no filled fields and no pk set. 

The pk will be created with the first field. It's important to know that we do not store any concept of "model", each field is totally independent, thought the keys to save them in Redis_ are based on the object's pk. So you can have 50 fields in a model and save only one of them.

Another really important thing to know is that when you create/retrieve an object, there is absolutely no data stored in it. Each time you access data via a field, the data is fetched from Redis_.

Model attributes
================

When defining a model, you will add fields, but there is also some other attributes that are mandatory or may be useful.

**database**

The `database` attribute is mandatory and must be a :doc:`RedisDatabase <database>` instance. See :doc:`database`

**namespace**

You can't have two models with the same name on the same database. Except if you use namespacing. 

Each model has a `namespace`, default to an empty string. 

The `namespace` can be used to regroup models. All models about registration could have the `namespace` "registration", ones about the payment could have "payment", and so on. 

With this you can have models with the same name in different `namespaces`, because the Redis_ keys created to store your data is computed with the `namespace`, the model name, and the pk of objects.

**abstract**

If you have many models sharing some field names, and/or within the same database and/or the same namespace, it could be useful to regroup all common stuff into a "base model", without using it to really store data in Redis_.

For this you have the `abstract` attribute, `False` by default::

    class Content(model.RedisModel):
        database = main_database
        namespace = "content"
        abstract = True

        title = fields.HashableField()
        pub_date = field.HashableField()

    class Article(Content):
        content = fields.StringField()

    class Image(Content):
        path = fields.HashableField()

In this example, only `Article` and `Image` are real models, both using the `main_database` database, the namespace "content", and having `title` and `pub_date` fields, in addition to their own.


**lockable**

By default, when updating a `indexable` field, update of the same field for all other instances of the model are locked while the update is not finished, to ensure consistency.

If you prefere speed, or are sure that you don't have more than one thread/process/server that write to the same database, you can set this `lockable` attribute to False to disable it for all the model's fields.

Note that you can also disable it at the field's level.

.. _Redis: http://redis.io
