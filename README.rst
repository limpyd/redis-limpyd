======
Limpyd
======

Idea is to provide an **easy** way to store objects in `Redis <http://redis.io/>`_, 
**without losing the power and the control of the Redis API**, in a *limpid* way. So, scope is to 
provide just as abstraction as needed.

For example:

- Don't care about keys, `limpyd` do it for you
- Retrieve objects from some of their attributes
- Retrieve objects collection
- CRUD abstraction
- Keep the power of all the `Redis data types <http://redis.io/topics/data-types>`_ in your own code

**This is full R&D, so *do not* try to use it in production right now!**

Example of configuration::

    from redis import model
    
    main_database = model.RedisDatabase(
        host="localhost",
        port=6379,
        db=0
    )

    class Bike(model.RedisModel):

        database = main_database

        name = model.HashableField(indexable=True, unique=True)
        color = model.HashableField()
        wheels = model.StringField(default=2)


So you can use it like this::

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

In the following documentation you'll find these topics:

- Database_
- Models_
- Fields_
- Collections_
- Cache_
- Contrib_
    - `Related fields`_
    - Pipelines_
    


.. _RedisDatabase:

********
Database
********

The first element to define when using `limpyd` is the database. The main goal of the database is to handle the conneciton to Redis_ and to host the models.

It's easy to define a database, as its arguments are the same as for a standard connection to Redis_ via `redis-py <https://github.com/andymccurdy/redis-py>`_::

    from limpyd.database import RedisDatabase
    
    main_database = RedisDatabase(host='localhost', port=6379, db=0)

Then it's also easy to define the database (which is mandatory) on which a model is defined::

    class Example(model.RedisModel):
        database = main_database
        some_field = fields.StringField()

If you have more than one model to host on a database, it's a good idea to create an abstract model::

    class BaseModel(model.RedisModel):
        database = main_database
        abstract = True

    class Foo(BaseModel):
        foo_field = fields.StringField()

    class Bar(BaseModel):
        bar_field = fields.StringField()

Note that you cannot have two models with the same name (the name of the class) in the same database (for obvious collusion problems), but we provide a namespace attribute on models to resolve this problem (so you can use an external module with models named as yours). See Models_ to know how to use them.

It's not a good idea to declare many RedisDatabase_ objects on the same Redis_ database (defined with host+port+db), because of obvious colusion problems if models have the same name in each. So do it only if you really know what you're doing, and with different models only.



.. _RedisModel:

******
Models
******

Models_ are the core of limpyd, it's why we're here. A RedisModel_ is a class, in a database, with some fields. Each instance of this model is a new object stored in Redis_ by `limpyd`.

Here a simple example::

    class Example(model.RedisModel):
        database = main_database

        foo = field.StringField()
        bar = field.StringField()

To create an instance, it's as easy as::

    >>> example = Example(foo='FOO', bar='BAR')

By just doing this, the fields are created, and a PKField_ is set with a value that you can use::

    >>> print "New example object with pk #%s" % example.pk.get()
    New example object with pk #1

Then later to get an instance from Redis_ with it's pk, it's as simple as:

    >>> example = Example(1)

So, to create an object, pass fields and their values as named arguments, and to retrieve it, pass its pk as the only argument. To retrieave instances via other fields than the pk, check the Collections_ section later in this document.

If you don't pass any argument to the RedisModel_, default one from fields are taken and are saved. But if no arguments and no default values, you get an empty instance, with no filled fields and no pk set. 

The pk will be created with the first field. It's important to know that we do not store any concept of "model", each field is totally independent, thought the keys to save them in Redis_ are based on the object's pk. So you can have 50 fields in a model and save only one of them.

Another really important thing to know is that when you create/retrieve an object, there is absolutely no data stored in it. Each time you access data via a field, the data is fetched from Redis_, except if you use the Cache_ (actually activated by default)

Model attributes
================

When defining a model, you will add fields, but there is also some other attributes that are mandatory or may be useful.

database
^^^^^^^^^

The `database` attribute is mandatory and must be a RedisDatabase_ instance. See Database_

namespace
^^^^^^^^^

You can't have two models with the same name on the same database. Except if you use namespacing. 

Each model has a `namespace`, default to an empty string. 

The `namespace` can be used to regroup models. All models about registration could have the `namespace` "registration", ones about the payment could have "payment", and so on. 

With this you can have models with the same name in different `namespaces`, because the Redis_ keys created to store your data is computed with the `namespace`, the model name, and the pk of objects.

abstract
^^^^^^^^^

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


cacheable
^^^^^^^^^

As we don't store field values in the object, and to avoid querying Redis_ each time we need a value, `limpyd` implements a level of local cache. It's activated by default, just set the `cacheable` attribute on the model to False to deactivate it.

See Cache_ for more informations about this local cache.



******
Fields
******

The core module of `limpyd` provides 5 fields types, matching the ones in Redis_:

- StringField_, for the main data type in `Redis`, strings
- HashableField_, for hashes
- SetField_, for sets
- ListField_, for lists
- SortedSetField_, for sorted sets

You can also manage primary keys with these too fields:

- PKField_, based on StringField_
- AutoPKField_, same as PKField_ but auto-incremented.

All these fields can be indexed, cached, and manage the keys for you (they take the same arguments as the real Redis_ ones, as defined in the `StrictRedis` class of `redis-py`_, but without the `key` parameter)

Another thing all fields have in common, is the way to delete them: use the `delete` method on a field, and both the field and its value will be removed from Redis_.



Field attributes
================

When adding fields to a model, you can configure it with some attributes:

cacheable
^^^^^^^^^

We provide a way to deactivate cache on a specific field is the cache is activated on the model. Simply pass the `cacheable` argument to False.

For more informations about the cache, check Cache_.


default
^^^^^^^

It's possible to set default values for fields of type StringField_ and HashableField_::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField(default='FOO')
        bar = fields.StringField()

    >>> example = Example(bar='BAR')
    >>> example.foo.get()
    'FOO'

When setting a default value, the field will be saved when creating the instance. If you defined a PKField_ (not AutoPKField_), don't forget to pass a value for it when creating the instance, it's needed to store other fields.


indexable
^^^^^^^^^

Sometimes getting objects from Redis_ by its primary key is not what you want. You may want to search for objects with a specific value for a specific field. 

By setting the `indexable` argument to True when defining the field, this functionnality is automatically activated, and you'll be able to retrieve objects by filtering on this field using Collections_.

To activate it, just set the `indexable` argument to True::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField(indexable=True)
        bar = fields.StringField()

In this example you will be able to filter on the field `foo` but not on `bar`.

See Collections_ to know how to filter objects.

unique
^^^^^^

The `unique` argument is the same as the `indexable` one, except it will ensure that you can't have multiple objects with the same value for some fields. `unique` fields are also indexed, and can be filtered, as for the `indexable` argument.

Example::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField(indexable=True)
        bar = fields.StringField(unique=True)

    >>> example1 = Example(foo='FOO', bar='BAR')
    True
    >>> example2 = Example(foo='FOO', bar='BAR')
    UniquenessError: Key :example:bar:BAR already exists (for instance 1)

See Collections_ to know how to filter objects, as for `indexable`.



Field types
===========


StringField
-----------

StringField_ based fields allow the storage of strings, but some `Redis string commands <http://redis.io/commands#string>`_ allow to treat them as integer, float or bits.

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        name = fields.StringField()

You can use this model like this::
    
    >>> example = Example(name='foo')
    >>> example.name.get()
    'foo'
    >>> example.name.set('bar')
    >>> example.name.get()
    'bar'
    >> example.delete()

The StringField_ type support these `Redis string commands`_:

Getters
^^^^^^^
- `get`
- `getbit`
- `getrange`
- `getset`
- `strlen`

Modifiers
^^^^^^^^^
- `append`
- `decr`
- `decrby`
- `getset`
- `incr`
- `incrby`
- `incrbyfloat`
- `set`
- `setbit`
- `setnx`
- `setrange`


HashableField
-------------

As for StringField_, HashableField_ based fields allow the storage of strings. But all the `HashableField` fields of an instance are stored in the same Redis_ hash, the name of the field being the key in the hash.

To fully use the power of Redis_ hashes, we also provide two methods to get and set multiples field in one operation (see hmget_ and hmset_). It's usually cheaper to store fields in hash that in strings. And it's faster to set/retrieve them using these two commands.

Example with simple commands::

    class Example(model.RedisModel):
        database = main_database

        foo = fields.HashableField()
        bar = fields.HashableField()

    >>> example.foo.hset('FOO')
    1  # 1 because the hash field was created
    >>> example.foo.hget()
    'FOO'

The HashableField_ type support these `Redis hash commands <http://redis.io/commands#hash>`_:

Getters
^^^^^^^
- hget

Modifiers
^^^^^^^^^
- `hincrby`
- `hincrbyfloat`
- `hset`
- `hsetnx`

Deleter
^^^^^^^
* Note that to delete the value of a HashableField_, you can use the `hdel` command, which do the same as the main `delete` one.

Multi
^^^^^

The two following commands are not called on the fields themselves, but on an instance.

- hmget_
- hmset_

hmget
"""""

hmget_ is called directly on an instance, and expects a list of field names to retrieve.

The result will be, as in Redis_, a list of all values, in the same order.

If no names are provided, all the HashableField_ based fields will be fetched.

It's up to you to associate names and values, but you can find an example below::

    class Example(model.RedisModel):
        database = main_database

        foo = fields.HashableField()
        bar = fields.HashableField()

        def hmget_dict(self, *args):
            """
            A call to hmget but which return a dict with field names as keys, instead
            of only a list of values
            """
            values = self.hmget(*args)
            keys = args or self._hashable_fields
            return dict(zip(keys, values))


    >>> example = Example(foo='FOO', bar='BAR')
    >>> example.hmget('foo', 'bar')
    ['FOO', 'BAR']
    >>> example.hmget_dict('foo', 'bar')
    {'bar': 'BAR', 'foo': 'FOO'}

hmset
"""""

hmset_ is the reverse of hmget_, and also called directly on an instance, and expects
named arguments with field names as keys, and new values to set as values.

Example (with same model as for hmget_)::

    >>> example = Example()
    >>> example.hmset(foo='FOO', bar='BAR')
    True
    >>> example.hmget('foo', 'bar')
    ['FOO', 'BAR']


SetField
--------

SetField_ based fields can store many values in one field, using the set data type of Redis_, an unordered set (with unique values).

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        stuff = fields.SetField()

You can use this model like this::
    
    >>> example = Example()
    >>> example.stuff.sadd('foo', 'bar')
    2  # number of values really added to the set
    >>> example.stuff.smembers()
    set(['foo', 'bar'])
    >>> example.stuff.sismember('bar')
    True
    >>> example.stuff.srem('bar')
    True
    >>> example.stuff.smembers()
    set(['foo'])
    >>> example.stuff.delete()
    True

The SetField_ type support these `Redis set commands <http://redis.io/commands#set>`_:

Getters
^^^^^^^
- `scard`
- `sismember`
- `smembers`
- `srandmember`

Modifiers
^^^^^^^^^
- `sadd`
- `spop`
- `srem`


ListField
---------

ListField_ based fields can store many values in one field, using the list data type of Redis_. Values are ordered, and are not unique (you can push many times the same value).

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        stuff = fields.ListField()

You can use this model like this::
    
    >>> example = Example()
    >>> example.stuff.rpush('foo', 'bar')
    2  # number of values added to the list
    >>> example.stuff.lrange(0, -1)
    ['foo', 'bar']
    >>> example.stuff.lindex(1)
    'bar'
    >>> example.stuff.lrem(1, 'bar')
    1  # number of values really removed
    >>> example.stuff.lrange(0, -1)
    ['foo']
    >>> example.stuff.delete()
    True

The ListField_ type support these `Redis list commands <http://redis.io/commands#list>`_:

Getters
^^^^^^^
- `lindex`
- `llen`
- `lrange`

Modifiers
^^^^^^^^^
- `linsert`
- `lpop`
- `lpush`
- `lpushx`
- `lrem`
- `lset`
- `ltrim`
- `rpop`
- `rpush`
- `rpushx`


SortedSetField
--------------

SortedSetField_ based fields can store many values, each scored, in one field using the sorted-set data type of Redis_. Values are unique (it's a set), and are ordered by their score.

Example::

    from limpyd import model, fields
    
    class Example(model.RedisModel):
        database = main_database
        
        stuff = fields.SortedSetField()

You can use this model like this::
    
    >>> example = Example()
    >>> example.stuff.zadd(foo=2.5, bar=1.1)
    2  # number of values added to the sorted set
    >>> example.stuff.zrange(0, -1)
    ['bar', 'foo']
    >>> example.stuff.zrangebyscore(1, 2, withscores=True)
    [('bar', 1.1)]
    >>> example.stuff.zrem('bar')
    1  # number of values really removed
    >>> example.stuff.zrangebyscore(1, 2, withscores=True)
    []
    >>> example.stuff.delete()
    True

The SortedSetField_ type support these `Redis sorted set commands <http://redis.io/commands#sorted_set>`_:

Getters
^^^^^^^
- `zcard`
- `zcount`
- `zrange`
- `zrangebyscore`
- `zrank`
- `zrevrange`
- `zrevrangebyscore`
- `zrevrank`
- `zscore`

Modifiers
^^^^^^^^^
- `zadd`
- `zincrby`
- `zrem`
- `zremrangebyrank`
- `zremrangebyscore`


PKField
-------

PKField_ is a special subclass of StringField_ that manage primary keys of models. The PK of an object cannot be updated, as it serves to create keys of all its stored fields. It's this PK that is returned, with others, in Collections_.

A PK can contain any sort of string you want: simple integers, float, long uuid, names...

If you want a PKField which will be automatically filled, and auto-incremented, see AutoPKField_. Otherwise, with standard PKField_, you must assign a value to it when creating an instance.

By default, a model has a AutoPKField_ attached to it, named `pk`. But you can redefine the nameand type of PKField you want.

Examples::

    class Foo(model.RedisModel):
        """
        The PK field is `pk`, and will be auto-incremented.
        """
        database = main_database

    class Bar(model.RedisModel):
        """
        The PK field is `id`, and will be auto-incremented.
        """
        database = main_database
        id = fields.AutoPKField()

    class Baz(model.RedisModel):
        """
        The PK field is `name`, and won't be auto-incremented, so you must assign it a value when creating an instance.
        """
        database = main_database
        name = fields.PKField()

Note that wathever name you use for the PKField_ (or AutoPKField_), you can always access it via the name `pk` (but also we its real name). It's easier for abstraction.

To access the pk value of an object, you have many ways::

    class Example(model.RedisModel):
        database = main_database
        id = fields.AutoPKField()
        name = fields.StringField()

    >>> example = Example(name='foobar')
    >>> example.get_pk()
    1
    >>> example.pk.get()
    1
    >>> example.id.get()
    1


AutoPKField
-----------

A AutoPKField_ field is a PKField_ filled with auto-incremented integers, starting to 1. Assigning a value to of AutoPKField_ is forbidden.

It's a AutoPKField_ that is attached by default to every model, if no other one is defined.

See PKField_ for more details.



***********
Collections
***********

The main and obvious way to get data from Redis_ via `limpyd` is to know the primary key of objects and instantiate them one by one.

But some fields can be indexed, passing them the `indexable` or `unique` attribute. 

If fields are indexed, it's possible to make query to retrieve many of them, using the `collection` method on the models.

The filtering has some limitations:

- you can only filter on fields with `indexable` and/or `unique` attributes set to True
- you can only filter on full values (`limyd` doesn't provide filters like "startswith", "contains"...)
- all filters are "and"ed
- no "not" (only able to find mathing fields, not to exlude some)
- no "join" (filter on one model only)

The result of a call to the `collection` is lazy. The query is only sent to Redis_ when data is really needed, to display or do computation with them.

By default, a collection returns a list of primary keys for all the matching objects, but you can sort them, retrieve only a part, and/or directly get full instances instead of primary keys.

We will explain Filtering_, Sorting_, Slicing_, Instanciating_, and Lazyness_ below, based on this example::

    class Person(model.RedisModel):
        database = main_database
        firstname = fields.HashableField(indexable=True)
        lastname = fields.HashableField(indexable=True)
        birth_year = fields.HashableField(indexable=True)

        def __repr__(self):
            return "<[%s] %s %s (%s)>" % tuple([self.get_pk()] + self.hmget('firstname', 'lastname', 'birth_year'))

    >>> Person(firstname='John', lastname='Smith', birth_year=1960)
    <[1] John Smith (1960)>
    >>> Person(firstname='John', lastname='Doe', birth_year=1965)
    <[2] John Doe (1965)>
    >>> Person(firstname='Emily', lastname='Smith', birth_year=1950)
    <[3] Emily Smith (1950)>
    >>> Person(firstname='Susan', lastname='Doe', birth_year=1960)
    <[4] Susan Doe (1960)>


Filtering
---------

To filter, simply call the `collection` (class)method with fields you want to filter as keys, and wanted values as values::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='john', lastname='Smith')
    ['1']
    >>> Person.collection(birth_year=1965)
    ['2']
    >>> Person.collection(birth_year=1965, lastname='Smith')
    []

You cannot pass two filters with the same name. All filters are "and"ed.


Slicing
-------

To slice the result, simply act as it's the result of a collection is a list::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John')[1:2]
    ['2']


Sorting
-------

With the help of the `sort` command of Redis_, `limpyd` is able to sort the result of collections.

It's as simple as calling the `sort` method of the collection. Use the `by` argument to specify on which field to sort.

Redis_ default sorting is numeric. If you want to sort values lexicographically, set the `alpha` parameter to True.

Example::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True)
    ['2', '1']
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True)[1:2]
    [1']
    >>> Person.collection().sort(by='birth_year')
    ['3', '1', '4', '2']




Instanciating
-------------

If you want to retrieve already instanciated objects, instead of only primary keys and having to do instanciation yourself, you simply have to call `instances()` on the result of the collection. The result of the collection and its methods (`sort` and `instances`) return a collection, so you can do chaining::

    >>> Person.collection(firstname='John')
    ['1', '2']
    >>> Person.collection(firstname='John').instances()
    [<[1] John Smith (1960)>, <[2] John Doe (1965)>]
    >>> Person.collection(firstname='John').instances().sort(by='lastname', alpha=True)
    [<[2] John Doe (1965)>, <[1] John Smith (1960)>]
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True).instances()
    [<[2] John Doe (1965)>, <[1] John Smith (1960)>]
    >>> Person.collection(firstname='John').sort(by='lastname', alpha=True).instances()[0]
    [<[2] John Doe (1965)>


Lazyness
--------

The result of a collection is lazy. In fact it's the collection itself, it's why we can chain calls to `sort` and `instances`.

The query is sent to Redis_ only when the data are needed. In the previous examples, data was needed to display them.

But if you do somthing like::

    >>> results = Person.collection(firstname='John').instances())

nothing will be done while results is not printed, iterated...



*****
Cache
*****

As we don't store field values in the object, and to avoid querying Redis_ each time we need a value, `limpyd` implements a level of local cache.


On the model
------------

This cache is activated by default for each model. To deactivate it, it's as simple as adding the attribute `cacheable` to False on the model::

    class Example(model.RedisModel):
        database = main_database
        cacheable = False

        a_field = fields.StringField()

The use of the cache is transparent. If you got a value from a field, without updating it after that, the next time you'll get it, the value will be fetched from the cache. When a field is updated, its cached is cleared.

Example::

    >>> example = Example()
    >>> example.a_field.set('foo')
    True
    >>> example.a_field.get()  # call Redis_
    'foo'
    >>> example.a_field.get()  # hit the cache
    'foo'
    >>> example.a_field.set('bar')  # clear the cache
    True
    >>> example.a_field.get()  # call Redis_
    'bar'


On fields
---------

If the cache is activated on the model, you can deactivate it at the field level. The reverse is not True (if the cache is deactivated for the model, you cannot activate it for a field).

To deactivate it for the field, just set the `cacheable` argument to True::

    class Example(model.RedisModel):
        database = main_database
        foo = fields.StringField()
        bar = fields.StringField(cacheable=False)

Here the cache is activated for `foo` but not for `bar`.


WARNING
-------

Be careful that the cache is on the instance itself. If you create another instance on the same object, update a field, the cache from the first instance will not be cleared. It's also obviously the case if you work with multiple threads of workers.



*******
Contrib
*******

To keep the core of `limpyd`, say, "limpid", we limited what it contains. But we added some extra stuff in the `contrib` module:

- `Related fields`_
- Pipelines_


Related fields
==============

*(documentation to come)*


Pipelines
=========

*(documentation to come)*
