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
- Keep the power of all the Redis data types in your own code



**This is full R&D, so *do not* try to use it in production right now!**


Example of configuration::

    from redis import model
    
    class Bike(model.RedisModel):
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
    
