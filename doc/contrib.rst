*******
Contrib
*******

To keep the core of `limpyd`, say, "limpid", we limited what it contains. But we added some extra stuff in the `contrib` module:

- `Related fields`_
- Pipelines_


Related fields
==============

`limpyd` provide a way to link models, via the `related` contrib module. It's only shortcuts to already existing stuff, aiming to make relations easy.

Start with an example:

.. code:: python

    from limpyd import fields
    from limpyd.contrib import related

    class Person(related.RelatedModel):
        database = main_database
        name = fields.PKField()  # redefine a PK just for the example

    class Group(related.RelatedModel):
        database = main_database
        name = fields.PKField()
        private = fields.StringField()
        owner = related.FKInstanceHashField('Person')
        members = related.M2MSetField('Person', related_name='membership')


With this we can do stuff like this:

.. code:: python

    >>> core_devs = Group(name='limpyd core devs', private=0)
    >>> ybon = Person(name='ybon')
    >>> twidi = Person(name='twidi')
    >>> core_devs.owner.hset(ybon)
    1
    >>> core_devs.members.sadd(twidi, ybon._pk)  # give a limpyd object, or a pk
    2
    >>> core_devs.members.smembers()
    set(['ybon', 'twidi'])
    >>> ybon.group_set(private=0)  # it's a collection, the limpyd way !
    ['limpyd core devs']
    >>> twidi.membership()  # it's a collection too
    ['limpyd core devs']


.. _RelatedModel:

Related model
-------------

To use related fields, you must use `related.RelatedModel` instead of `model.RedisModel`. It handles creation of `related collections` and manage propagation of deletion for us.

Related field types
-------------------

The `related` module provides 5 field types, based on the standard ones. All have the `indexable` attribute set to True.

There is one big addition on these fields over the normal one. Everywhere you can pass a value to store (in theory you would pass an object's primary key), you can pass an instance of a limpyd model. The primary key of these instances will be extraced for you.

Here are the new field types:

FKStringField
"""""""""""""

The FKStringField_ type is based on :ref:`StringField` and allow setting a foreign key.

It just stores the primary key of the related object in a :ref:`StringField`.

FKInstanceHashaField
""""""""""""""""""""

The FKInstanceHashaField_ type is based on :ref:`InstanceHashField` and allow setting a foreign key.

It works like FKStringField_ but, as a :ref:`InstanceHashField`, can be retrieved with other fields via the :ref:`InstanceHashField-hmget` method on the instance.

M2MSetField
"""""""""""

The M2MSetField_ type is based on :ref:`SetField` and allow setting many foreign keys, acting as a Many 2 Many fields.

If no order is needed, it's the best choice for M2M, because it's the lightest M2M field (memory occupation), and it's fast to check if an element is included (`sismember`, O(1)), or to remove one (`srem`, O(N) where N is the number of members to be removed.).

If you need ordering *and* unicity, check M2MSortedSetField_.

M2MListField
""""""""""""

The M2MListField_ type is based on :ref:`ListField` and allow setting many foreign keys, acting as a Many 2 Many fields.

It works like M2MSetField_, with two differences, because it's a list and not a set:

- the list of foreign keys is ordered
- we can have many times the same foreign key

This type is usefull to keep the order of the foreign keys, but as it does not ensure unicity, the use cases are less obvious.

If you need ordering *and* unicity, check M2MSortedSetField_.

M2MSortedSetField
"""""""""""""""""

The M2MSortedSetField_ type is based on :ref:`SortedSetField` and allow setting many foreign keys, acting as a Many 2 Many fields.

It works like M2MSetField_, with one differences, because it's a sorted set and not a simple set: each foreign key has a score attached to it, and the list for foreign keys is sorted by this score.

This score is usefull to keep the entries unique AND sorted. It can be a date (as a timestamp because the score must be numeric), allowing, in our example (Person/Group), to keep list of members in the order they joined the group.

Related field arguments
------------------------

The related fields accept two new arguments when declaring them. One to tell to which model it's related (to_), and one to give a name to the `related collection`_

to
"""

The first new argument (and the first in the list of accepted ones, useful to pass it without naming it), is `to`, the name of the model on which this field is related to.

Note that the related model must be on the same :doc:`database`.

It can accept a RelatedModel_:

.. code:: python

    class Person(related.RelatedModel):
        database = main_database
        name = StringField()

    class Group(related.RelatedModel):
        database = main_database
        name = StringField()
        owner = FKStringField(Person)

In this case the :ref:`RelatedModel` must be defined before the current model.

And it can accept a string. There is two ways to define model with a string:

- the name of a RelatedModel_:

.. code:: python

    class Group(related.RelatedModel):
        database = main_database
        owner = FKStringField('Person')

If you want to link to a model with a different namespace than the one for the current model, you can add it:

.. code:: python

    class Group(related.RelatedModel):
        database = main_database
        owner = FKStringField('my_namespace:Person')

- 'self', to define a link to the same model on which the related field is defined:

.. code:: python

    class Group(related.RelatedModel):
        database = main_database
        parent = FKStringField('self')


related_name
""""""""""""

The `related_name` argument is not mandatory, except in some cases described below.

This argument is the name which will be used to create the `Related collection`_ on the related model (the on described by the to_ argument)

If defined, it must be a string. This string can accept to formatable arguments: `%(namespace)s` and `%(model)s` which will be replaced by the namespace and name of the model on which the related field is defined. It's usefull for subclassing:

.. code:: python

    class Person(related.RelatedModel):
        database = main_database
        name = StringField()

    class BaseGroup(related.RelatedModel):
        database = main_database
        namespace = 'groups'
        abstract = True

        name = StringField()
        owner = FKStringField('Person', related_name='%(namespace)s_%(model)s_set')

    class PublicGroup(BaseGroup):
        pass

    class PrivateGroup(BaseGroup):
        pass

In this example, a person will have two related collections:

- `groups_publicgroup_set`, liked to the `parent` field of `PublicGroup`
- `groups_privategroup_set`, liked to the `parent` field of `PrivateGroup`

Note that, exept for namespace that will be automatically converted if needed, related names should be valid python identifiers.

Related collection
------------------

Related collections are the other side of the relation. They are created on the related model, based on the related_name_ argument used when creating the related field.

They are a shortcut to the real collection, but available to ease writing.

Let's define some models:

.. code:: python


    class Person(related.RelatedModel):
        database = main_database
        name = PKStringField()

    class Group(related.RelatedModel):
        database = main_database
        name = PKStringField()
        private = fields.StringField(defaut=0)
        owner = FKStringField('Person', related_name='owned_groups')

Now we can do:

.. code:: python

    >>> group1 = Group(name='group 1')
    >>> group2 = Group(name='group 1', private=1)
    >>> person1 = Person(name='person 1')
    >>> group1.owner.set(person1)
    >>> group2.owner.set(person1)

To retrieve groups owned by `person1`, we can use the standard way:

.. code:: python

    >>> Group.collection(owner=person1.pk.get())
    ['group 1', 'group 2']

or, with the related collection:

.. code:: python

    >>> person1.owned_groups()
    ['group 1', 'group 2']

These two lines return exactly the same thing, a lazy collection (See :doc:`collections`).

You can pass other filters too:

.. code:: python

    >>> person1.owned_groups(private=1)
    ['group 2']

Note that the collection manager of all related fields is the ExtendedCollectionManager_, so you can do things like:

.. code:: python

    >>> owned = person1.owned_groups()
    >>> owned.filter(private=1)
    ['group 2']


Retrieving the other side
-------------------------

Foreign keys
""""""""""""

It's easy to set a foreign key, and easy to retrieve it using the default API.

Using these models:

.. code:: python

    class Person(related.RelatedModel):
        database = main_database
        name = StringField()

    class Group(related.RelatedModel):
        database = main_database
        name = StringField()
        owner = FKStringField(Person)

We can add data:

.. code:: python

    >>> core_devs = Group(name='limpyd core devs', private=0)
    >>> ybon = Person(name='ybon')
    >>> core_devs.owner.hset(ybon)

And we can retrieve the related object this way:

.. code:: python

    >>> owner_pk = core_devs.owner.hget()
    >>> owner = Person(owner_pk)

But we can use the `instance` method defined on foreign keys:

.. code:: python

    >>> owner = core_devs.owner.instance()


Many to Many
""""""""""""

To provide consistency on calling collections on the both sides of a relation, the M2MSetField_, M2MListField_ and M2MSortedSetField_ are `callable`, simulating a call to a collection, and effectively returning one. It's very useful to sort and/or return `instances`, `values` or `values_list`.

Imagine the model:

.. code:: python

    class Person(related.RelatedModel):
        database = main_database
        name = PKStringField()
        following = M2MSetField('self', related_name='followers')

Let's add some data:

.. code:: python

    >>> foo = Person(name='Foo')  # pk=1
    >>> bar = Person(name='Bar')  # pk=2
    >>> baz = Person(name='Baz')  # pk=3
    >>> foo.following.sadd(bar, baz)
    >>> baz.following.sadd(bar)

So we can retrieve followers via the `Related collection`_:

.. code:: python

    >>> bar.followers()
    ['1', '3']
    >>> baz.followers().values_list('name', flat=True)
    ['foo', 'baz']

And on the other side... without simulating a collection when calling a M2MField, it's easy to retrieve primary keys:

.. code:: python

    >>>foo.following.smembers()
    ['2', '3']

But it's not the same "api" (but it sounds ok because it's a SetField), and it's really hard to retrieve names, or other stuff like with `values` and `values_list`, or even `instances`.

With the callable possibility added to M2M fields, you can do this:

.. code:: python

    >>> foo.following()  # return a collection
    ['1', '3']
    >>> foo.following().values_list('name', flat=True)
    ['bar', 'baz']

Note that to provide even more consitency, use can call the `collection` method of a M2M field instead of simple "calling" it. So both lines below are the same:

.. code:: python

    >>> foo.following()
    >>> foo.following.collection()


Update and deletion
-------------------

One of the main advantage of using related fields instead of doing it yourself, is that updates and deletions are handled as you would, transparently.

In the previous example, if the owner of a group is updated (or deleted), the previous owner doesn't have this group in its owned_group collections.

The same applies on the other side. If a person who is the owner of a group is deleted, the value of the groups'owner field is deleted too.

And it works with M2M fields too.



Pipelines
=========

In the contrib module, we provide a way to wirk with pipelines as defined in `redis-py`_, providimg abstraction to let the fields connect to the pipeline, not the real Redis_ connection (this won't be the case if you use the default pipeline in `redis-py`_)

To activate this, you have to import and to use `PipelineDatabase` instead of the default `RedisDatabase`, without touching the arguments.

Instead of doing this:

.. code:: python

    from limpyd.database import RedisDatabase

    main_database = RedisDatabase(
        host="localhost",
        port=6379,
        db=0
    )

Just do:

.. code:: python

    from limpyd.contrib.database import PipelineDatabase

    main_database = PipelineDatabase(
        host="localhost",
        port=6379,
        db=0
    )

This `PipelineDatabase` class adds two methods: pipeline_ and transaction_

pipeline
--------

The pipeline provides the same functionnalities as for the default pipeline in `redis-py`_, but it handles transparently the use of the pipeline instead of the default collection for all fields operation.

But be aware that within a pipeline you cannot get values from fields to do something with them. It's because in a pipeline, all commands are sent in bulk, and all results are retrieved in bulk too (one for each command), when exiting the pipeline.

It does not mean that you cannot set many fields in one time in a pipeline, but you must have values not depending of other fields, and, also very important, you cannot update indexable fields ! (so no related fields either, because they are all indexable)

The best use for pipelines in `limpyd`, is to get a lot of values in one pass.

Say we have this model:

.. code:: python

    from limpyd.contrib.database import PipelineDatabase

    main_database = PipelineDatabase(
        host="localhost",
        port=6379,
        db=0
    )

    class Person(model.RedisModel):
        database = main_database
        namespace='foo'
        name = fields.StringField()
        city = fields.StringField(indexable=True)

Add some data:

.. code:: python

    Person(name='Jean Dupond', city='Paris')
    Person(name='Francois Martin', city='Paris')
    Person(name='John Smith', city='New York')
    Person(name='John Doe', city='San Franciso')
    Person(name='Paul Durand', city='Paris')

Say we have already a lot of Person saved, we can retrieve all names this way:

.. code:: python

    persons = list(Person.collection(city='Paris').instances())
    with main_database.pipeline() as pipeline:
        for person in persons:
            person.name.get()
        names = pipeline.execute()
    print names

This will result in only one call (within the pipeline):

.. code:: python

    >>> ['Jean Dupond', 'Francois Martin', 'Paul Durand']

All in one only call to the Redis_ server.

Note that in pipelines you can you the `watch` command, but it's easier to use the `transaction` method described below.

transaction
-----------

The `transaction` method available on the `PipelineDatabase` object, is the same as the one in `redis-py`_, but using its own `pipeline` method.

The goal is to help using pipelines with watches.

The `watch` mechanism in Redis_ allow us to read values and use them in a pipeline, being sure that the values got in the first step were not updated by someone else since we read them.

Imagine the `incr` method doesn't exists. Here is a way to implement it with a transaction without race condition (ie without the risk of having our value updated by someone else between the moment we read it, and the moment we save it):

.. code:: python

    class Page(model.RedisModel):
        database = main_database  # a PipelineDatabase object
        url = fields.StringField(indexable=True)
        hits = fields.StringField()

        def incr_hits(self):
            """
            Increment the number of hits without race condition
            """

            def do_incr(pipeline):

                # transaction not started, we can read values
                previous_value = self.hits.get()

                # start the transaction (MANDATORY CALL)
                pipeline.multi()

                # set the new value
                self.hits.set(previous_value+1)


            # run `do_incr` in a transaction, watching for the hits field
            self.database.transaction(do_incr, *[self.hits])

In this example, the `do_incr` method will be aborted and executed again, restarting the transaction, each time the `hits` field of the object is updated elsewhere. So we are absolutely sure that we don't have any race conditions.

The argument of the `transaction` method are:

- **func**, the function to run, encaspulated in a transaction. It must accept a `pipeline` argument.
- **\*watches**, a list of keys to watch (if a watched key is updated, the transaction is restarted and the function aborted and executed again). Note that you can pass keys as string, or fields of limpyd model instances (so their keys will be retrieved for you).

The `transaction` method returns the value returned by the execution of its internal pipeline. In our example, it will return `[True]`.

Note that as for the `pipeline` method, you cannot update indexables fields in the transaction because read commands are used to update them.


.. _ExtendedCollectionManager:

Extended collection
===================

Although the standard collection may be sufficient in most cases, we added an ExtendedCollectionManager_ in contrib, which enhance the base one with some useful stuff:

- ability to retrieve values as dict or liist of tuples
- ability to chain filters
- ability to intersect the final result with a list of primary keys
- ability to sort by the score of a sorted set
- ability to pass fields on some methods
- ability to store results

To use this ExtendedCollectionManager_, declare it as seen in :ref:`collection-subclassing`.

All of these new capabilities are described below:


Retrieving values
=================

If you don't want only primary keys, but instances are too much, or too slow, you can ask the collection to return values with two methods: `values` and `values_list` (inspired by django)

It can be really useful to quickly iterate on all results when you, for example, only need to display simple values.

**values**

When calling `values` on a collection, the result of the collection is not a list of primary keys, but a list of dictionaries, one for each matching entry, with each field passed as argument. If no field is passed, all fields are retrieved. Note that only simple fields (:ref:`PKField`, :ref:`StringField` and :ref:`InstanceHashField`) are concerned.

Example:

.. code:: python

    >>> Person.collection(firstname='John').values()
    [{'pk': '1', 'firstname': 'John', 'lastname': 'Smith', 'birth_year': '1960'}, {'pk': '2', 'firstname': 'John', 'lastname': 'Doe', 'birth_year': '1965'}]
    >>> Person.collection(firstname='John').values('pk', 'lastname')
    [{'pk': '1', 'lastname': 'Smith'}, {'pk': '2', 'lastname': 'Doe'}]


**values_list**

The `values_list` method works the same as `values` but instead of having the collection return a list of dictionaries, it will return a list of tuples with values for asked fields, in the same order as they are passed as arguments. If no field is passed, all fields are retrieved in the same order as they are defined in the model.

Example:

.. code:: python

    >>> Person.collection(firstname='John').values_list()
    [('1', 'John', 'Smith', '1960'), (2', 'John', 'Doe', '1965')]
    >>> Person.collection(firstname='John').values_list('pk', 'lastname')
    [('1', 'Smith'), ('2', 'Doe')]

If you want to retrieve a single field, you can ask to get a flat list as a final result, by passing the `flat` named argument to True:

.. code:: python

    >>> Person.collection(firstname='John').values_list('pk', 'lastname')  # without flat
    [('Smith', ), ('Doe', )]
    >>> Person.collection(firstname='John').values_list('lastname', flat=True)  # with flat
    ['Smith', 'Doe']


To cancel retrieving values and get the default return format, call the `primary_keys` method:

.. code:: python

    >>> Person.collection(firstname='John').values().primary_keys()  # works with values_list too
    >>> ['1', '2']


Chaining filters
----------------

With the standard collection, you can chain method class but you cannot add more filters than the ones defined in the `collecion` method. The only way was to create a dictionary, populate it, then pass it as named arguments:

.. code:: python

    >>> filters = {'firstname': 'John'}
    >>> if want_to_filter_by_city:
    >>>     filters['city'] = 'New York'
    >>> collection = Person.collection(**filters)

With the ExtendedCollectionManager_ available in `contrib.collection`, you can add filters after the initial call:

.. code:: python

    >>> collection = Person.collection(firstname='John')
    >>> if want_to_filter_by_city:
    >>>     collection.filter(city='New York')

`filter` return the collection object itself, so it can be chained.

Note that all filters are ANDed, so if you pass two filters on the same field, you may have an empty result.


Intersections
-------------

Say you already have a list of primary keys, maybe got from a previous filter, and you want to get a collection with some filters but matching this list. With ExtendedCollectionManager_, you can easily do this with the `intersect` method.

This `intersect` method takes a list of primary keys and will intersect, if possible at the Redis_ level, the result with this list.

`intersect` return the collection itself, so it can be chained, as all methods of a collection. You may call this method many times to intersect many lists, but you can also pass many lists in one `intersect` call.

Here is an example:

.. code:: python

    >>> my_friends = [1, 2, 3]
    >>> john_people = list(Person.collection(firstname='John'))
    >>> my_john_friends_in_newyork = Person.collection(city='New York').intersect(john_people, my_friends)

`intersect` is powerful as it can handle a lot of data types:

- a python list
- a python set
- a python tuple
- a string, which must be the key of a Redis_ set, sorted_set or list (long operation if a list)
- a `limpyd` :ref:`SetField`, attached to a model
- a `limpyd` :ref:`ListField`, attached to a model
- a `limpyd` :ref:`SortedSetField`, attached to a model

Imagine you have a list of friends in a :ref:`SetField`, you can directly use it to intersect:

.. code:: python

    >>> # current_user is an instance of a model, and friends a SetField_
    >>> Person.collection(city='New York').intersect(current_user.friends)


Sort by score
-------------

Sorted sets in Redis_ are a powerful feature, as it can store a list of data sorted by a score. Unfortunately, we can't use this score to sort via the Redis_ `sort` command, which is used in `limpyd` to sort collections.

With ExtendedCollectionManager_, you can do this using the `sort` method, but with the new `by_score` named argument, instead of the `by` one used in simple sort.

The `by_score` argument accepts a string which must be the key of a Redis_ sorted set, or a :ref:`SortedSetField` (attached to an instance)

Say you have a list of friends in a sorted set, with the date you met them as a score. And you want to find ones that are in you city, but keep them sorted by the date you met them, ie the score of the sorted set. You can do this this way:

.. code:: python

    # current_user is an instance of a model, with city a field holding a city name
    # and friends, a sorted_set with Person's primary keys as value, and the date
    # the current_user met them as score.

    >>> # start by filtering by city
    >>> collection = Person.collection(city=current_user.city.get())
    >>> # then intersect with friends
    >>> collection.intersect(current_user.friends)
    >>> # finally keep sorting by friends meet date
    >>> collection.sort(by_score=current_user.friends)

With the sort by score, as you have to use the `sort` method, you can still use the `alpha` and `desc` arguments (see :ref:`collection-sorting`)

When using `values` or `values_list` (see `Retrieving values`_), you may want to retrieve the score between other fields. To do so, simply use the SORTED_SCORE constant (defined in `contrib.collection`) as a field name to pass to `values` or `values_list`:

.. code:: python

    >>> from limpyd.contrib.collection import SORTED_SCORE
    >>> # (following previous example)
    >>> collection.sort(by_score=current_user.friends).values('name', SORTED_SCORE)
    [{'name': 'John Smith', 'sorted_score': '1985.0'}]  # here 1985.0 is the score


Passing fields
--------------

In the standard collection, you must never pass fields, only names and values, depending on the methods.
In the `contrib` module, we already allow passing fields in some place, as to set FK and M2M in `Related fields`_.

Now you can do this also in collection (if you use ExtendedCollectionManager_):

- the `by` argument of the `sort` method can be a field, and not only a field name
- the `by_score` arguement of the `sort` method can be a :ref:`SortedSetField` (attached to an instance), not only the key of a Redis_ sorted set
- arguments of the `intersect` method can be python list(etc...) but also multi-values `RedisField`
- the right part of filters (passed when calling `collection` or `filter`) can also be a `RedisField`, not only a value. If a `RedisField` (specifically a `SingleValueField`), its value will be fetched from Redis_ only when the collection will be really called


Storing
-------

For collections with heavy computations, like multiple filters, intersecting with list, sorting by sorted set, it can be useful to store the results.

It's possible with ExtendedCollectionManager_, simply by calling the `store` method, which take two optional arguments:

- `key`, which is the key where the result will be stored, default to a randomly generated one
- `ttl`, the duration, in seconds, for which we want to keep the stored result in Redis_, default to `DEFAULT_STORE_TTL` (60 seconds, defined in `contrib.collection`). You can pass None if you don't want the key to expire in Redis_.

When calling `store`, the collection is executed and you got a new ExtendedCollectionManager_ object, pre-filled with the result of the original collection.

Note that only primary keys are stored, even if you called `instances`, `values` or `values_list`. But arguments for these methods are set in the new collection so if you call it, you'll get what you want (instances, dictionaries or tuples). You can call `primary_keys` to reset this.

If you need the key where the data are stored, you can get it by calling the `stored_key` method on the new collection. With it, you can later create a collection based on this key.

One important thing to note: the new collection is based on a Redis_ list. As you can add filters, or intersections, like any collection, remember that by doing this, the list will be converted into a set, which can take time. It's preferable to do this on the original collection before sorting (but it's possible and you can always store the new filtered collection into an other one.)

A last word: if the key is already expired when you execute the new collection, a `DoesNotExist` exception will be raised.

An example to show all of this, based on the previous example (see `Sort by score`_):

.. code:: python

    >>> # Start by making a collection with heavy calculation
    >>> collection = Person.collection(city=current_user.city.get())
    >>> collection.intersect(current_user.friends)
    >>> collection.sort(by_score=current_user.friends)

    >>> # then store the result
    >>> stored_collection = collection.store(ttl=3600)  # keep the result for one hour
    >>> # get, say, pk and names
    >>> page_1 = stored_collection.values('pk', 'name')[0:10]

    >>> # get the stored key
    >>> stored_key = stored_collection.stored_key

    >>> # later (less than an hour), in another process (passing the stored_key between the processes is let as an exercise for the reader)
    >>> stored_collection = Person.collection().from_stored(stored_key)
    >>> page_2 = stored_collection.values('pk', 'name')[10:20]

    >>> # want to extend the expire time of the key ?
    >>> my_database.connection.expire(store_key, 36000)  # 10 hours
    >>> # or remove this expire time ?
    >>> my_database.connection.persist(store_key)


.. _Redis: http://redis.io
.. _redis-py: https://github.com/andymccurdy/redis-py
