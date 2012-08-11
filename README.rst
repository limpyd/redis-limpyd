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
    >>> tricycle = Bike(name="tricycle")
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
    

********
Database
********

*(documentation to come)*

******
Models
******

*(documentation to come)*

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

All these fields can be indexed, cached, and manage the keys for you (they take the same arguments as the real `Redis` ones, as defined in the `StrictRedis` class of `redis-py <https://github.com/andymccurdy/redis-py>`_, but without the `key` parameter)

Another thing all fields have in common, is the way to delete them: use the `delete` method on a field, and both the field and its value will be removed from Redis_.

StringField
===========

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
*******
- `get`
- `getbit`
- `getrange`
- `getset`
- `strlen`

Modifiers
*********
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
=============

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

Getters:
********
- hget

Modifiers:
**********
- `hincrby`
- `hincrbyfloat`
- `hset`
- `hsetnx`

Deleter:
********
* Note that to delete the value of a HashableField_, you can use the `hdel` command, which do the same as the main `delete` one.

Multi:
******

The two following commands are not called on the fields themselves, but on an instance.

- hmget_
- hmset_

hmget
-----

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
-----

hmset_ is the reverse of hmget_, and also called directly on an instance, and expects
named arguments with field names as keys, and new values to set as values.

Example (with same model as for hmget_)::

    >>> example = Example()
    >>> example.hmset(foo='FOO', bar='BAR')
    True
    >>> example.hmget('foo', 'bar')
    ['FOO', 'BAR']


SetField
========

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

Getters:
********
- `scard`
- `sismember`
- `smembers`
- `srandmember`

Modifiers:
**********
- `sadd`
- `spop`
- `srem`


ListField
=========

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

Getters:
********
- `lindex`
- `llen`
- `lrange`

Modifiers:
**********
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
==============

*(documentation to come)*

PKField
=======

*(documentation to come)*

AutoPKField
===========

*(documentation to come)*

***********
Collections
***********

*(documentation to come)*

*****
Cache
*****

*(documentation to come)*

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
